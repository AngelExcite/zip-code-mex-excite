from fastapi import APIRouter, HTTPException
from app.db.connection import client
from pathlib import Path
import csv

from app.models.zip_code import ZipCode

router = APIRouter()


@router.get("/get-by-cp/{cp}", response_model=list[ZipCode])
async def get_by_cp(cp):
    db = client.zip_codes_mex
    codes_collection = db["codes"]
    cps = codes_collection.find({"cp": int(cp)})

    return list(cps)
    # return {"cp": cp, "data": cps}


@router.get("/load")
async def load_zip_codes():
    db = client.zip_codes_mex
    codes_collection = db["codes"]
    # drop codes collection
    codes_collection.drop()
    codes_collection = db["codes"]

    # read CPdescarga.txt and convert to latin-1 to UTF-8
    script_location = Path(__file__).absolute().parent
    file_location = script_location / 'CPdescarga.txt'
    with open(file_location, 'r', encoding='ISO-8859-15') as f:
        lines = f.readlines()
    with open(script_location / 'CPdescarga-utf8.txt', 'w', encoding='utf-8') as f:
        f.writelines(lines)

    file_location = script_location / 'CPdescarga-utf8.txt'

    codes_json = []

    with open(file_location) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        line_count = 0
        for row in csv_reader:
            if line_count < 2:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:

                codes_json.append({
                    "cp": int(row[0]),
                    "colonia": row[1],
                    "municipio": row[3],
                    "estado": row[4]
                })
                line_count += 1

    print(f"Insertando {line_count} documentos...")
    print("Esto tardará...")
    print("No parar el proceso")

    codes_collection.insert_many(codes_json)

    return {"message": f"{line_count} documentos insertados"}
