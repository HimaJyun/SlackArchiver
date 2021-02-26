# SlackArchiver
Archive old posts and unused files as a human readable.

# Install
You need [Python](https://www.python.org/) and [pipenv](https://pypi.org/project/pipenv/).

```
git clone https://github.com/HimaJyun/SlackArchiver.git
cd SlackArchiver
pipenv sync
pipenv run python slackarchiver.py --help
```

# Token
Need **User token** (starts with xoxp), Not **Bot token** (starts with xoxb).

## Scope
Note: Direct messages can't be deleted. (Slack API doesn't support it)

### Archive
- users:read
- channels:history (for public channel)
- groups:history (for private channel)
- im:history (for direct message)
- mpim:history (for multi-person direct message)

### Archive unused files
- files:read

### Clean up
- chat:write
- files:read
- files:write
