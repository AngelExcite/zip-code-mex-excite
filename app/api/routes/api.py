from fastapi import APIRouter
from app.api.routes import zip_codes

router = APIRouter()

router.include_router(
    zip_codes.router, tags=["Zip Codes"], prefix="/zip-codes"
)
