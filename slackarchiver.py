import os
import slack_sdk
import dataclasses
import json
import random
import requests
import hashlib
import datetime
import time
from datetime import datetime
from collections.abc import (Callable, Iterator)

slack: slack_sdk.WebClient = None


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


def file_download(path: os.PathLike, url: str, ext: str) -> str:
    file = requests.get(url=url,
                        allow_redirects=True,
                        headers={
                            "Authorization": "Bearer " + slack.token
                        }).content
    digest = hashlib.sha256(file).hexdigest()
    p = os.path.join(path, digest + "." + ext)
    if os.path.exists(p):
        return digest
    with open(p, mode="wb") as f:
        f.write(file)
    return digest


def msg_parser(obj: dict) -> MessageObject:
    if obj["type"] != "message":
        raise TypeError('"type" is not "message"')
    ts = datetime.fromtimestamp(int(obj["ts"].split(".")[0]))
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


def cursor_history(channel: str, before: datetime) -> Iterator[dict]:
    history = limit_call(lambda: slack.conversations_history(
        channel=channel, latest=str(before.timestamp())))
    # SlackResponce
    while True:
        if not history.get("ok", False):
            raise ValueError("responce is not ok")
        yield from history["messages"]

        # カーソル進める
        if not history.get("has_more", False):
            break
        history = limit_call(lambda: slack.conversations_history(
            channel=channel,
            cursor=history["response_metadata"]["next_cursor"]))


def message_write(path: os.PathLike, messages: list[MessageObject]):
    os.makedirs(path, exist_ok=True)
    # 古い順でソートし直す
    messages.sort(key=lambda m: m.time)

    # 整形しながら出力
    with open(os.path.join(path, "log.txt"), mode="a", encoding="utf-8") as f:
        for m in messages:
            f.write(f"TODO <{m.user}>: {m.time.isoformat()}\n")
            f.write(m.message)
            f.write("\n\n")


def archive(channel: str, out: os.PathLike, before: datetime,
            splitter: Callable[[datetime], str]):
    raw = {"messages": [], "threads": []}
    split: str = None
    buf: list[MessageObject] = []
    for v in cursor_history(channel, before):
        raw["messages"].append(v)
        message = msg_parser(v)
        current = splitter(message.time)
        # 溜めて一括処理
        if current == split:
            buf.append(message)
            continue
        elif split is None:
            buf.append(message)
            split = current
            continue

        # ファイルに書き出す
        message_write(os.path.join(out, split), buf)

        # 忘れずに入れ直す
        split = current
        buf = [message]

    # 余り
    if len(buf) != 0:
        message_write(os.path.join(out, split), buf)

    with open(os.path.join(out, "raw-{0:%Y%m%d-%H%M%S}.json".format(
            datetime.now())),
              mode="x",
              encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=4)


def main():
    global slack
    slack = slack_sdk.WebClient(token=os.environ["SLACK_TOKEN"])
    archive(channel="TODO",
            out="./TODO",
            before=datetime.now(),
            splitter=get_splitter("month"))


if __name__ == "__main__":
    main()
