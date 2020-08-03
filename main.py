import os
from typing import List, Optional
import time
import logging

from twitter import Api, Status, User
from slack import WebClient

logger = logging.getLogger(__name__)


def pull_and_publish(
    consumer_key: str,
    consumer_secret: str,
    access_token: str,
    access_token_secret: str,
    slack_token: str,
    slack_channel: str,
    wait_time: int,
):
    """Continuously pull recent Twitter statuses and publish them to Slack."""

    twitter_api = Api(consumer_key, consumer_secret, access_token, access_token_secret)

    slack_client = WebClient(slack_token)
    users_to_exclude = ['Ethereum_MXN', 'someOtherUser', 'someOtherUser2']

    since_id = None
    while True:
        #statuses = twitter_api.GetSearch(raw_query="q=-Precio%20(from%3Abitso)%20(to%3Abitso)%20(%40bitso)&src=typed_query&f=live")
        statuses = twitter_api.GetSearch(raw_query="q=bitso%20&result_type=recent&since_id={}".format(since_id))

        if statuses:
            logger.info(f"Got {len(statuses)} statuses from Twitter.")
            count = 0
            publishable_statuses = set()
            for status in reversed(statuses):
                user = status.user
                screen_name = user.screen_name
                if any(u in screen_name for u in users_to_exclude):
                    count += 1

                else:
                    twitter_link = f"http://twitter.com/{user.screen_name}/status/{status.id}"
                    _publish(slack_channel, slack_client, twitter_link, user)

                since_id = status.id

            logger.info(f"Skipped {count} statuses from excluded users.")

        else:
            logger.info("No new twitter statuses.")

        time.sleep(wait_time)


def _publish(
    slack_channel: str,
    slack_client: WebClient,
    twitter_link: str,
    user: User,
    message: str = "View on Twitter",
):
    """Format the slack post text and publish to slack_channel."""
    slack_post_text = f"<{twitter_link}|{message}>"

    slack_client.chat_postMessage(
        text=slack_post_text,
        channel=slack_channel,
        icon_url=user.profile_image_url,
        username=user.name,
    )

    logger.info(f"Posted status from {user.name} to slack on '{slack_channel}'.")
    time.sleep(5)  # give slack time to format the posts


def _retrieve_keys() -> List[str]:
    """Retrieve the necessary keys to communicate with Twitter and Slack APIs."""
    env_vars = (
        "TWITTER_CONSUMER_KEY",
        "TWITTER_CONSUMER_SECRET",
        "TWITTER_ACCESS_TOKEN",
        "TWITTER_ACCESS_TOKEN_SECRET",
        "SLACK_API_TOKEN",
        "TWITTER_ON_SLACK_CHANNEL",
    )

    keys = []

    for env_var in env_vars:
        value = os.environ.get(env_var)
        if value is None:
            raise KeyError(f"Missing required env var: {env_var}")

        keys.append(value)
    return keys


def main(wait_time: int = 120):
    """Continuously pull twitter statuses and publish them to a slack channel."""
    keys = _retrieve_keys()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(asctime)s %(message)s",
        datefmt="%m-%d %H:%M:%S",
    )
    pull_and_publish(*keys, wait_time=wait_time)


if __name__ == "__main__":
    main()
