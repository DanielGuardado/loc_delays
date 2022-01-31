import pyodbc

from config import (
    CONNECTION2,
)


class Db:
    def __init__(self):
        self.cnxn = pyodbc.connect(CONNECTION2)
        self.cursor = self.cnxn.cursor()
        self.cursor.fast_executemany = True

    def select_po(self, po):
        self.cursor.execute(
            "SELECT PO FROM PurchaseOrder where po = ? and delayed_status = 0",
            po,
        )
        return [item[0] for item in self.cursor.fetchall()]

    def select_po_inserted(self, po):
        self.cursor.execute(
            "SELECT PO, delayed_date FROM PurchaseOrder where po = ? and delayed_status = 1",
            po,
        )
        return [item for item in self.cursor.fetchone()]

    def select_po_items(self, po):
        self.cursor.execute(
            "SELECT Sku, Qty FROM PurchaseOrderItem where po = ?",
            po,
        )
        return [item for item in self.cursor.fetchall()]

    def update_status(self, po, date):
        self.cursor.execute(
            "UPDATE PurchaseOrder SET delayed_status = 1, delayed_date = ? where po = ?",
            date,
            po,
        )
        self.cursor.commit()

    def update_status(self, po, date):
        self.cursor.execute(
            "UPDATE PurchaseOrder SET delayed_status = 1, delayed_date = ? where po = ?",
            date,
            po,
        )
        self.cursor.commit()
