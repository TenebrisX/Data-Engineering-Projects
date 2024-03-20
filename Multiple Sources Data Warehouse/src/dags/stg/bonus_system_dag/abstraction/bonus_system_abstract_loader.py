from abc import ABC, abstractmethod
from logging import Logger
from lib import PgConnect
from stg import StgEtlSettingsRepository


class BonusAbstractLoader(ABC):
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = self.create_origin_repository(pg_origin)
        self.destination = self.create_destination_repository()
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

