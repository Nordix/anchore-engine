from datetime import datetime

from anchore_engine.db import GrypeDBFeedMetadata, session_scope
from anchore_engine.services.policy_engine.engine.feeds.grypedb_sync import (
    GrypeDBSyncManager,
)


class TestGrypeDbSyncManager:
    def test_resolve_too_many_active_dbs(self, test_data_env):
        grype_dbs = []
        with session_scope() as db:
            for i in range(3):
                grype_db = GrypeDBFeedMetadata(
                    archive_checksum=f"test-checksum-{i}",
                    schema_version="2",
                    object_url="test-url",
                    active=True,
                    built_at=datetime.utcnow(),
                )
                grype_dbs.append(grype_db)
                db.add(grype_db)
            db.commit()

            active_db = GrypeDBSyncManager.resolve_and_get_active_grypedb(db)
            assert active_db.archive_checksum == "test-checksum-2"
            assert active_db.active is True
            assert (
                db.query(GrypeDBFeedMetadata)
                .filter(GrypeDBFeedMetadata.active == True)
                .count()
                == 1
            )
            assert db.query(GrypeDBFeedMetadata).count() == 3
