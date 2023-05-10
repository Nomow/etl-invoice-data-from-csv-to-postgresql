from sqlalchemy import Column, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class BaseModel(Base):

    __abstract__ = True
    created_at: DateTime = Column(DateTime, server_default=func.now())
    updated_at: DateTime = Column(DateTime, server_default=func.now(), onupdate=func.now())
