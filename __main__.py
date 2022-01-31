from datetime import datetime, timedelta
from config import api_key
import requests
import pandas as pd
from db import Db
import pysftp
from config import ftp_data_aa, ftp_data_local
import pathlib
import os
from dateutil import parser


def main():
    today = datetime.today()
    recent_date = (today - timedelta(2)).strftime("%Y-%m-%d")
    url = f"{api_key['endpoint']}salesorder?filter=updated_at>=\"{recent_date}\"&customer_id=7"
    print(url)
    raw_response = requests.get(
        url,
        auth=api_key["key"],
    )
    response = raw_response.json()

    po_stack = []
    for item in response["data"]:
        memo = item.get("memo")
        memo = "no" if memo is None else memo
        if "backorder" in memo.lower():
            po_stack.append([item["number"][2:], item["scheduled_fulfillment_date"]])
    print(po_stack)

    db = Db()
    final_df = []
    for row in po_stack:
        po, scheduled_fulfillment_date = row
        status = db.select_po(po)

        if status:
            items = db.select_po_items(po)
            for item in items:
                final_df.append(
                    {
                        "po_number": po,
                        "sku": item[0],
                        "quantity": item[1],
                        "new_estimated_ship_date": scheduled_fulfillment_date,
                        # "new_estimated_ship_date": "20200214",
                        "notice_date": datetime.today().strftime("%Y%m%d"),
                    }
                )
        elif not status:
            po, db_date = db.select_po_inserted(po)
            loc_date = parser.parse(scheduled_fulfillment_date)
            if loc_date.date() != db_date.date():
                items = db.select_po_items(po)
                for item in items:
                    final_df.append(
                        {
                            "po_number": po,
                            "sku": item[0],
                            "quantity": item[1],
                            "new_estimated_ship_date": scheduled_fulfillment_date,
                            # "new_estimated_ship_date": "20200214",
                            "notice_date": datetime.today().strftime("%Y%m%d"),
                        }
                    )

    df = pd.DataFrame.from_dict(final_df)
    print(df)
    if not df.empty:
        filename = f"tmp/Delay_{datetime.today().strftime('%m%d%Y')}.csv"
        # filename = f"tmp/test.csv"
        df.to_csv(filename, index=False)

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = pysftp.Connection(
            host=ftp_data_aa["host"],
            username=ftp_data_aa["username"],
            password=ftp_data_aa["password"],
            cnopts=cnopts,
        )
        file_path = rf"{pathlib.Path().resolve()}\{filename}"
        sftp.cwd("Inbound/")
        # sftp.cwd("AATest/")
        if os.path.exists(file_path):
            print(file_path)
            sftp.put(localpath=file_path, confirm=False)
        sftp.close()
        os.remove(file_path)

        for row in po_stack:
            po, scheduled_fulfillment_date = row
            date_scheduled = parser.parse(scheduled_fulfillment_date)
            db.update_status(po, date_scheduled)


if __name__ == "__main__":
    main()
