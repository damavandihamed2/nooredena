from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, Integer, SmallInteger, String, Text
from .base import Base


class CodalAllReports(Base):
    __tablename__ = "all_reports"
    __table_args__ = {"schema": "codal"}

    tracing_number: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    company: Optional[str] = Column(String(255), nullable=True)
    company_name: Optional[str] = Column(String(255), nullable=True)
    title: Optional[str] = Column(String(512), nullable=True)
    letter_code: Optional[str] = Column(String(50), nullable=True)
    has_html: Optional[int] = Column(SmallInteger, nullable=True)
    is_estimate: Optional[int] = Column(SmallInteger, nullable=True)
    url: Optional[str] = Column(String(255), nullable=True)
    has_excel: Optional[int] = Column(SmallInteger, nullable=True)
    has_pdf: Optional[int] = Column(SmallInteger, nullable=True)
    has_attachment: Optional[int] = Column(SmallInteger, nullable=True)
    attachment_url: Optional[str] = Column(String(255), nullable=True)
    pdf_url: Optional[str] = Column(String(255), nullable=True)
    excel_url: Optional[str] = Column(String(255), nullable=True)
    under_supervision: Optional[int] = Column(SmallInteger, nullable=True)
    additional_info: Optional[str] = Column(String(512), nullable=True)
    reasons: Optional[str] = Column(Text, nullable=True)
    sent_date: Optional[str] = Column(String(50), nullable=True)
    sent_time: Optional[str] = Column(String(50), nullable=True)
    publish_date: Optional[str] = Column(String(50), nullable=True)
    publish_time: Optional[str] = Column(String(50), nullable=True)


class CodalAuditors(Base):
    __tablename__ = "auditors"
    __table_args__ = {"schema": "codal"}

    auditor_name: Optional[str] = Column(String(255), nullable=True)
    auditor_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)


class CodalCategories(Base):
    __tablename__ = "categories"
    __table_args__ = {"schema": "codal"}

    category_code: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    category_name: Optional[str] = Column(String(100), nullable=True)
    publisher_code: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    publisher_name: Optional[str] = Column(String(50), nullable=True)
    letter_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    letter_name: Optional[str] = Column(String(255), nullable=True)


class CodalCompanies(Base):
    __tablename__ = "companies"
    __table_args__ = {"schema": "codal"}

    company: Optional[str] = Column(String(255), nullable=True)
    company_name: Optional[str] = Column(String(255), nullable=True)
    company_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company_type: Optional[int] = Column(Integer, nullable=True)
    company_state: Optional[int] = Column(Integer, nullable=True)
    industry_id: Optional[int] = Column(Integer, nullable=True)
    report_type: Optional[int] = Column(Integer, nullable=True)


class CodalCompanyState(Base):
    __tablename__ = "company_state"
    __table_args__ = {"schema": "codal"}

    company_state: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    company_state_name: Optional[str] = Column(String(255), nullable=True)


class CodalCompanyType(Base):
    __tablename__ = "company_type"
    __table_args__ = {"schema": "codal"}

    company_type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    company_type_name: Optional[str] = Column(String(255), nullable=True)


class CodalFinancialYear(Base):
    __tablename__ = "financial_year"
    __table_args__ = {"schema": "codal"}

    financial_year: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class CodalIndustryGroup(Base):
    __tablename__ = "industry_group"
    __table_args__ = {"schema": "codal"}

    industry_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    industry_name: Optional[str] = Column(String(255), nullable=True)


class CodalReportType(Base):
    __tablename__ = "report_type"
    __table_args__ = {"schema": "codal"}

    report_type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    report_type_name: Optional[str] = Column(String(255), nullable=True)


class CodalTsetmcMapper(Base):
    __tablename__ = "tsetmc_mapper"
    __table_args__ = {"schema": "codal"}

    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company: Optional[str] = Column(String(155), nullable=True)
    company_name: Optional[str] = Column(String(255), nullable=True)
    company_id: Optional[str] = Column(String(10), nullable=True)
    company_type: Optional[int] = Column(Integer, nullable=True)
    company_state: Optional[int] = Column(Integer, nullable=True)
