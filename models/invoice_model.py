from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class InvoiceModel(Base):

    __tablename__ = "invoices"
    index : Integer = Column(Integer, primary_key=True, index=True)
    invoice_date : DateTime = Column(DateTime)
    customer_name : String = Column(String)
    price : Float = Column(Float)
    quantity : Integer = Column(Integer)
    item_name : String = Column(String)
    tax_rate : Float = Column(Float)
    total_amount : Float = Column(Float)

