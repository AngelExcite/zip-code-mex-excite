from pydantic import BaseModel


class ZipCode(BaseModel):
    cp: int
    colonia: str
    municipio: str
    estado: str
