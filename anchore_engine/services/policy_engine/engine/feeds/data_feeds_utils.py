from typing import Dict, List, Optional, Union

from anchore_engine.common.schemas import (
    FeedAPIGroupRecord,
    FeedAPIRecord,
    GrypeDBListing,
)
from anchore_engine.db import FeedGroupMetadata, FeedMetadata
from anchore_engine.db import get_thread_scoped_session as get_session
from anchore_engine.services.policy_engine.engine.feeds import IFeedSource
from anchore_engine.services.policy_engine.engine.feeds.db import get_all_feeds
from anchore_engine.subsys import logger


class DataFeedsUtils:
    @staticmethod
    def get_grype_db_listing(
        feed_group_information, grypedb_feed_name
    ) -> GrypeDBListing:
        for feed_name, feed_api_record in feed_group_information.items():
            if feed_name == grypedb_feed_name:
                return next(group.grype_listing for group in feed_api_record["groups"])

    @staticmethod
    def _pivot_and_filter_feeds_by_config(
        to_sync: list, source_found: list, db_found: list
    ):
        """

        :param to_sync: list of feed names requested to be synced
        :param source_found: list of feed names available as returned by the upstream source
        :param db_found: list of db records that were updated as result of upstream metadata sync (this is to handle db update failures)
        :return:
        """
        available = set(to_sync).intersection(set(source_found))
        return {x.name: x for x in db_found if x.name in available}

    @staticmethod
    def _sync_feed_metadata(
        db,
        feed_api_record,
        db_feeds,
        operation_id: Optional[str] = None,
    ) -> None:
        api_feed = feed_api_record["meta"]
        db_feed = db_feeds.get(api_feed.name)
        # Do this instead of a db.merge() to ensure no timestamps are reset or overwritten
        if not db_feed:
            logger.debug(
                "Adding new feed metadata record to db: {} (operation_id={})".format(
                    api_feed.name, operation_id
                )
            )
            db_feed = FeedMetadata(
                name=api_feed.name,
                description=api_feed.description,
                access_tier=api_feed.access_tier,
                enabled=True,
            )
            db.add(db_feed)
            db.flush()
        else:
            logger.debug(
                "Feed metadata already in db: {} (operation_id={})".format(
                    api_feed.name, operation_id
                )
            )

    @staticmethod
    def _sync_feed_group_metadata(
        db,
        feed_api_record,
        db_feeds,
        operation_id: Optional[str] = None,
    ):
        api_feed = feed_api_record["meta"]
        db_feed = db_feeds.get(api_feed.name)
        # Check for any update
        db_feed.description = api_feed.description
        db_feed.access_tier = api_feed.access_tier

        db_groups = {x.name: x for x in db_feed.groups}
        for api_group in feed_api_record.get("groups", []):
            db_group = db_groups.get(api_group.name)
            # Do this instead of a db.merge() to ensure no timestamps are reset or overwritten
            if not db_group:
                logger.debug(
                    "Adding new feed metadata record to db: {} (operation_id={})".format(
                        api_group.name, operation_id
                    )
                )
                db_group = FeedGroupMetadata(
                    name=api_group.name,
                    description=api_group.description,
                    access_tier=api_group.access_tier,
                    feed=db_feed,
                    enabled=True,
                )
                db_group.last_sync = None
                db.add(db_group)
            else:
                logger.debug(
                    "Feed group metadata already in db: {} (operation_id={})".format(
                        api_group.name, operation_id
                    )
                )

            db_group.access_tier = api_group.access_tier
            db_group.description = api_group.description

    @staticmethod
    def sync_metadata(
        source_feeds,
        to_sync: list = None,
        operation_id: Optional[str] = None,
        groups: bool = True,
    ) -> tuple:
        """
        Get metadata from source and sync db metadata records to that (e.g. add any new groups or feeds)
        Executes as a unit-of-work for db, so will commit result and returns the records found on upstream source.

        If a record exists in db but was not found upstream, it is not returned

        :param feed_client:
        :param to_sync: list of string feed names to sync metadata on
        :return: tuple, first element: dict of names mapped to db records post-sync only including records successfully updated by upstream, second element is a list of tuples where each tuple is (failed_feed_name, error_obj)
        """

        if not to_sync:
            return {}, []

        db = get_session()
        try:
            logger.info(
                "Syncing feed and group metadata from upstream source (operation_id={})".format(
                    operation_id
                )
            )
            failed = []
            db_feeds = DataFeedsUtils._pivot_and_filter_feeds_by_config(
                to_sync, list(source_feeds.keys()), get_all_feeds(db)
            )

            for feed_name, feed_api_record in source_feeds.items():
                try:
                    logger.info(
                        "Syncing metadata for feed: {} (operation_id={})".format(
                            feed_name, operation_id
                        )
                    )
                    DataFeedsUtils._sync_feed_metadata(
                        db, feed_api_record, db_feeds, operation_id
                    )
                    if groups:
                        DataFeedsUtils._sync_feed_group_metadata(
                            db, feed_api_record, db_feeds, operation_id
                        )
                except Exception as e:
                    logger.exception("Error syncing feed {}".format(feed_name))
                    logger.warn(
                        "Could not sync metadata for feed: {} (operation_id={})".format(
                            feed_name, operation_id
                        )
                    )
                    failed.append((feed_name, e))
                finally:
                    db.flush()

            # Reload
            db_feeds = DataFeedsUtils._pivot_and_filter_feeds_by_config(
                to_sync, list(source_feeds.keys()), get_all_feeds(db)
            )

            db.commit()
            logger.info(
                "Metadata sync from feeds upstream source complete (operation_id={})".format(
                    operation_id
                )
            )
            return db_feeds, failed
        except Exception as e:
            logger.error(
                "Rolling back feed metadata update due to error: {} (operation_id={})".format(
                    e, operation_id
                )
            )
            db.rollback()
            raise

    def get_groups_to_download(self, feeds_to_sync, operation_id):
        # Do the fetches
        groups_to_download = []
        for f in feeds_to_sync:
            logger.info(
                "Initialized feed to sync: {} (operation_id={})".format(
                    f.__feed_name__, operation_id
                )
            )
            if f.metadata:
                if f.metadata.enabled:
                    for g in f.metadata.groups:
                        if g.enabled:
                            groups_to_download.append(g)
                        else:
                            logger.info(
                                "Will not sync/download group {} of feed {} because group is explicitly disabled".format(
                                    g.name, g.feed_name
                                )
                            )
                else:
                    logger.info(
                        "Skipping feed {} because it is explicitly not enabled".format(
                            f.__feed_name__
                        )
                    )
            else:
                logger.warn(
                    "No metadata found for feed {}. Unexpected but not an error (operation_id={})".format(
                        f.__feed_name__, operation_id
                    )
                )
            return groups_to_download
