import argparse
import dataclasses
import datetime
import json
import os
import random
import shutil
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta

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


def assert_ok(responce: slack_sdk.web.SlackResponse):
    if not responce.get("ok", False):
        raise ValueError("responce is not ok")


def user_name(user: str) -> str:
    if user in user_cache:
        return user_cache[user]

    u = limit_call(lambda: slack.users_info(user=user))
    assert_ok(u)

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


def file_download(dir_path: os.PathLike, file: FileObject):
    name = file.id
    ext = file.name.split(".", 1)
    if len(ext) == 2:
        name += "." + ext[1]
    path = os.path.join(dir_path, name)
    # 既にあれば何もしない
    if os.path.exists(path):
        return

    res = requests.get(url=file.url,
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
        assert_ok(v)
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
            continue
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
            file_download(path, f)
        for t in m.thread:
            for f in t.files:
                file_download(path, f)

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

    raw = {"channel": channel, "messages": [], "threads": {}}

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

    with open(os.path.join(
            out, "history-{0:%Y%m%d-%H%M%S}.json".format(datetime.now())),
              mode="x",
              encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=4)


def unused(out: os.PathLike, before: datetime, splitter: Callable[[datetime],
                                                                  str]):
    def download(file: dict):
        split = splitter(datetime.fromtimestamp(file["created"], tz=local_zone))
        path = os.path.join(out, split)
        os.makedirs(path, exist_ok=True)
        file_download(
            path,
            FileObject(id=file["id"],
                       name=file["name"],
                       title=file["title"],
                       url=file["url_private_download"]))

    files = []
    page = 1
    ts = str(int(before.timestamp()))
    while True:
        res = limit_call(lambda: slack.files_list(ts_to=ts, page=str(page)))
        assert_ok(res)
        # 使われてないファイルを探す
        for f in res["files"]:
            if "url_private_download" not in f:
                continue
            elif bool(f["channels"]) or bool(f["groups"]) or bool(f["ims"]):
                continue
            else:
                files.append(f)
                download(f)
        # ページング
        page += 1
        if page > res["paging"]["pages"]:
            break

    with open(os.path.join(
            out, "unused-{0:%Y%m%d-%H%M%S}.json".format(datetime.now())),
              mode="x",
              encoding="utf-8") as f:
        json.dump({"files": files}, f, ensure_ascii=False, indent=4)


def clean(file: os.PathLike,
          run: bool,
          only_files: bool = False,
          ignore_use: bool = False):
    data = {}
    with open(file, mode="r", encoding="utf-8") as f:
        data = json.load(f)

    channel: str = data.get("channel")

    def file_delete(f_id: str):
        if not run:
            return
        try:
            res = limit_call(lambda: slack.files_delete(file=f_id))
            assert_ok(res)
            print(f"delete file: {f_id}")
        except slack_sdk.errors.SlackApiError as e:
            if e.response["error"] == "file_not_found" or e.response[
                    "error"] == "file_deleted":
                print(f"not found: {f_id}")
                return
            else:
                raise e

    def chat_delete(chat: dict):
        if chat["type"] != "message":
            raise TypeError('"type" is not "message"')
        if run and not only_files:
            try:
                res = limit_call(
                    lambda: slack.chat_delete(channel=channel, ts=chat["ts"]))
            except slack_sdk.errors.SlackApiError as e:
                if e.response["error"] == "message_not_found":
                    ts = chat["ts"]
                    print(f"not found: {ts}")
                    return
                else:
                    raise e
            assert_ok(res)

        # 先にチャットを消さないと使用判定が上手く行かない
        for f in chat.get("files", []):
            if "url_private_download" not in f:
                continue
            f_id = f["id"]
            if run and not ignore_use:
                # 使われていないか確認
                try:
                    res = limit_call(lambda: slack.files_info(file=f_id))
                except slack_sdk.errors.SlackApiError as e:
                    if e.response["error"] == "file_not_found":
                        print(f"not found: {f_id}")
                        return
                    else:
                        raise e
                assert_ok(res)
                f_i = res["file"]
                if bool(f_i["channels"]) or bool(f_i["groups"]) or bool(
                        f_i["ims"]):
                    continue
            file_delete(f_id)

    for tt, tv in data.get("threads", {}).items():
        for t in tv:
            t_ts: str = t["ts"]
            if not only_files:
                print(f"delete chat: {channel} {tt} {t_ts}")
            chat_delete(t)

    for m in data.get("messages", []):
        m_ts: str = m["ts"]
        if not only_files:
            print(f"delete chat: {channel} {m_ts}")
        chat_delete(m)

    for f in data.get("files", []):
        if "url_private_download" not in f:
            continue
        file_delete(f["id"])


def main():
    global slack

    def before(days: int) -> datetime:
        return datetime.now() - timedelta(days=days)

    parser = argparse.ArgumentParser(description="Slack archiver")
    parser.add_argument("-t", "--token", help="Slack API Token")
    sub_parser = parser.add_subparsers()

    sub = sub_parser.add_parser("archive", help="archive posts")
    sub.add_argument("-o",
                     "--out",
                     type=str,
                     default="./history",
                     help="Output directory")
    sub.add_argument("-b",
                     "--before",
                     type=int,
                     default=0,
                     help="Archive data older than the specified days")
    sub.add_argument("-s",
                     "--split",
                     type=str,
                     default="month",
                     choices=["day", "month", "year", "all"],
                     help="Period to split the directory")
    sub.add_argument("channel", type=str, help="channel id")
    sub.set_defaults(func=lambda a: archive(channel=a.channel,
                                            out=a.out,
                                            before=before(a.before),
                                            splitter=get_splitter(a.split)))

    sub = sub_parser.add_parser("unused", help="archive unused files")
    sub.add_argument("-o",
                     "--out",
                     type=str,
                     default="./unused",
                     help="Output directory")
    sub.add_argument("-b",
                     "--before",
                     type=int,
                     default=0,
                     help="Archive data older than the specified days")
    sub.add_argument("-s",
                     "--split",
                     type=str,
                     default="month",
                     choices=["day", "month", "year", "all"],
                     help="Period to split the directory")
    sub.set_defaults(func=lambda a: unused(
        out=a.out, before=before(a.before), splitter=get_splitter(a.split)))

    sub = sub_parser.add_parser("clean", help="delete post")
    sub.add_argument(
        "-f",
        "--only-files",
        action="store_true",
        help="only file deleting. If specified, --ignore-use is also enabled.")
    sub.add_argument("-i",
                     "--ignore-use",
                     action="store_true",
                     help="delete even files that are in use")
    sub.add_argument("--summer-bugs-entering-the-fire",
                     action="store_true",
                     help="If specified, the operation will be executed." +
                     " Deleted posts cannot be restored." +
                     " ACCEPTED ALL RISKS!!")
    sub.add_argument("file", type=str, help="archive json file")
    sub.set_defaults(
        func=lambda a: clean(file=a.file,
                             run=a.summer_bugs_entering_the_fire,
                             only_files=a.only_files,
                             ignore_use=(a.ignore_use or a.only_files)))

    args = parser.parse_args()
    if args.token is not None:
        slack = slack_sdk.WebClient(token=args.token)
    elif os.getenv("SLACK_TOKEN") is not None:
        slack = slack_sdk.WebClient(token=os.getenv("SLACK_TOKEN"))
    else:
        slack = slack_sdk.WebClient(token=input("Slack API Token> "))

    # APIレベルで無効化しておく
    if hasattr(args, "summer_bugs_entering_the_fire"
              ) and not args.summer_bugs_entering_the_fire:
        slack = None

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
