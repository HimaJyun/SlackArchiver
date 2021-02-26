import dataclasses
import datetime
import json
import os
import random
import shutil
import time
from collections.abc import Callable, Iterator
from datetime import datetime

import requests
import slack_sdk

slack: slack_sdk.WebClient = None
local_zone = datetime.now().astimezone().tzinfo
user_cache = {}


@dataclasses.dataclass
class FileObject:
    id: str
    name: str
    title: str
    url: str


@dataclasses.dataclass
class MessageObject:
    time: datetime
    user: str
    message: str
    files: list[FileObject]
    thread_ts: str
    thread: list = dataclasses.field(default_factory=list)


def limit_call(func: callable) -> slack_sdk.web.SlackResponse:
    while True:
        try:
            ret = func()
            return ret
        except slack_sdk.errors.SlackApiError as e:
            if e.response["error"] == "ratelimited":
                time.sleep(1 + int(e.response.headers["Retry-After"]))
            else:
                raise e


def user_name(user: str) -> str:
    if user in user_cache:
        return user_cache[user]

    u = limit_call(lambda: slack.users_info(user=user))
    if not u.get("ok", False):
        raise ValueError("responce is not ok")

    v = u["user"]["real_name"]
    user_cache[user] = v
    return v


def get_splitter(value: str) -> Callable[[datetime], str]:
    if value == "day":
        return lambda date: date.strftime("%Y-%m-%d")
    elif value == "month":
        return lambda date: date.strftime("%Y-%m")
    elif value == "year":
        return lambda date: date.strftime("%Y")
    elif value == "all":
        return lambda date: "all"
    else:
        raise ValueError(f"{value} is invalid error")


def file_download(url: str, path: os.PathLike):
    res = requests.get(url=url,
                       allow_redirects=True,
                       stream=True,
                       headers={"Authorization": "Bearer " + slack.token})
    if res.status_code != 200:
        raise RuntimeError(f"HTTP Error: {res.status_code}")
    with open(path, mode="wb") as f:
        res.raw.decode_content = True
        shutil.copyfileobj(res.raw, f)


def cursor_pagination(first: callable,
                      cursor: callable,
                      mapper: callable = lambda v: v["messages"],
                      yield_from: bool = True) -> Iterator:
    v = limit_call(first)
    # SlackResponce
    while True:
        if not v.get("ok", False):
            raise ValueError("responce is not ok")
        if yield_from:
            yield from mapper(v)
        else:
            yield mapper(v)

        # カーソル進める
        if not v.get("has_more", False):
            break
        v = limit_call(lambda: cursor(v["response_metadata"]["next_cursor"]))


def msg_parser(obj: dict) -> MessageObject:
    if obj["type"] != "message":
        raise TypeError('"type" is not "message"')
    ts = datetime.fromtimestamp(float(obj["ts"]), tz=local_zone)
    msg = obj["text"]  # TODO: URL周りのパース
    f = []
    for v in obj.get("files", []):
        if "url_private_download" not in v:
            raise ValueError("Unknown files")
        f.append(
            FileObject(id=v["id"],
                       name=v["name"],
                       title=v["title"],
                       url=v["url_private_download"]))

    return MessageObject(time=ts,
                         user=obj["user"],
                         message=msg,
                         files=f,
                         thread_ts=obj.get("thread_ts", None))


def message_write(path: os.PathLike, messages: list[MessageObject]):
    os.makedirs(path, exist_ok=True)
    # 古い順でソートし直す
    messages.sort(key=lambda m: m.time)

    def download(file: FileObject):
        i = file.id
        s = file.name.split(".", 1)
        if len(s) != 1:
            i += "." + s[1]
        p = os.path.join(path, i)
        if not os.path.exists(p):
            file_download(file.url, p)

    def build_str(obj: MessageObject) -> list[str]:
        l = []
        l.append(
            f"{user_name(obj.user)} <{obj.user}>: {obj.time.isoformat()}\n")
        if obj.message:
            l.append(obj.message)
            l.append("\n")
        # ファイル
        for file in obj.files:
            if file.name == file.title:
                l.append(f"<{file.id}|{file.name}>\n")
            else:
                l.append(f"<{file.id}|{file.name}|{file.title}>\n")
        return l

    # まず最初にファイルをダウンロード
    for m in messages:
        for f in m.files:
            download(f)
        for t in m.thread:
            for f in t.files:
                download(f)

    # 整形しながら出力
    with open(os.path.join(path, "_log.txt"), mode="a", encoding="utf-8") as f:
        for m in messages:
            f.writelines(build_str(m))
            # スレッド
            for t in m.thread:
                for l in "".join(build_str(t)).splitlines(keepends=True):
                    f.write("> ")
                    f.write(l)
            f.write("\n")


def archive(channel: str, out: os.PathLike, before: datetime,
            splitter: Callable[[datetime], str]):
    def history() -> Iterator[dict]:
        latest = str(before.timestamp())
        return cursor_pagination(
            lambda: slack.conversations_history(channel=channel, latest=latest),
            lambda c: slack.conversations_history(channel=channel, cursor=c))

    def thread(ts: str) -> Iterator[dict]:
        return cursor_pagination(
            lambda: slack.conversations_replies(channel=channel, ts=ts),  # yapf
            lambda c: slack.conversations_replies(
                channel=channel, ts=ts, cursor=c))

    raw = {"messages": [], "threads": {}}

    split: str = None
    path: os.PathLike = None
    messages: list[MessageObject] = []
    for v in history():
        raw["messages"].append(v)
        message = msg_parser(v)
        # スレッドの処理
        if message.thread_ts is not None:
            raw["threads"].setdefault(message.thread_ts, [])
            for t in thread(message.thread_ts):
                # 同じ = スレッドの親なので無視する
                if t["ts"] != t["thread_ts"]:
                    raw["threads"][message.thread_ts].append(t)
                    message.thread.append(msg_parser(t))

        current = splitter(message.time)
        # 溜めて一括処理
        if current == split:
            messages.append(message)
            continue
        elif split is None:
            messages.append(message)
            split = current
            path = os.path.join(out, current)
            continue

        # ファイルに書き出す
        message_write(path, messages)

        # 忘れずに入れ直す
        split = current
        path = os.path.join(out, current)
        messages = [message]

    # 余り
    if len(messages) != 0:
        message_write(path, messages)

    with open(os.path.join(out,
                           "raw-{0:%Y%m%d-%H%M%S}.json".format(datetime.now())),
              mode="x",
              encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=4)


def main():
    global slack
    slack = slack_sdk.WebClient(token=os.environ["SLACK_TOKEN"])
    archive(channel="TODO",
            out="./TODO",
            before=datetime.now(),
            splitter=get_splitter("all"))


if __name__ == "__main__":
    main()
