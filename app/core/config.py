from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_string: str

    model_config = SettingsConfigDict(env_file=".env")


def get_settings():
    return Settings()
