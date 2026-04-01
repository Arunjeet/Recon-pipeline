from db_config import engine
from sqlalchemy import select,func,text,bindparam
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
import pandas as pd
from transform import trigger_TB
from transform import trigger_pnl
from transform import trigger_journals
from transform import trigger_manualjournals
from transform import master_data
from transform import trigger_account
import argparse


#--------------------------------------------------------------------------------------------------------

INSERT_MASTER = text("""INSERT INTO master(tenant_id)
VALUES (:tenant_id)
""")

def master_load():
    df = master_data()
    rows = df.to_dict(orient="records")

    # GETS UPDATED ONCE STARTS INSERTING...
    inserted = 0

    # Does use transaction...
    with SessionLocal.begin() as session:
        try:
            # ensure the code reads df['tenant_id']
            tenant_ids = df['tenant_id'].tolist()

            if tenant_ids:
                delete_stmt = (
                    text("DELETE FROM master WHERE tenant_id IN :ids")
                    .bindparams(bindparam("ids", expanding=True))
                )
                session.execute(delete_stmt, {"ids": tenant_ids})

            stmt = INSERT_MASTER
            session.execute(stmt, rows)
            inserted = len(rows)

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted, df

#--------------------------------------------

#FOR TB

INSERT_TB_CLIENT_STG = text("""INSERT INTO tb_client_stg (tenant_id, row_hash, date, section, label, debit, credit, accountid, accountcode) 
VALUES (:tenant_id, :row_hash, :date, :section, :label, :debit, :credit, :accountid, :accountcode)
""")

def load_TB(date_str: str):

    df = trigger_TB(date_str)
    rows = df.to_dict(orient="records")

    #GETS UPDATED ONCE STARTS INSERTING...
    inserted = 0

    # Does use transaction...
    with SessionLocal.begin() as session:
        try:      
            stmt=INSERT_TB_CLIENT_STG
            session.execute(stmt, rows)
            inserted = len(rows)


            session.execute(text("""
            INSERT INTO tb_client(tenant_id, row_hash, date, section, label, debit, credit, accountid, accountcode)            
            SELECT tenant_id, row_hash, date, section, label, debit, credit, accountid, accountcode from tb_client_stg
            WHERE NOT EXISTS(
            SELECT 1 FROM tb_client a 
            WHERE a.row_hash=tb_client_stg.row_hash);""" ))

            #Use staging table to maintain ids...
            session.execute(text("""
            UPDATE tb_client AS c
            SET
            tenant_id    = s.tenant_id,
            date        = s.date,
            section     = s.section,
            label       = s.label,
            debit       = s.debit,
            credit      = s.credit,
            accountid   = s.accountid,
            accountcode = s.accountcode
            FROM tb_client_stg AS s
            WHERE s.row_hash = c.row_hash;""" ))


            # Delete from staging table...
            session.execute(text("""DELETE FROM tb_client_stg""" ))
            session.commit()
            
        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted,df

#------------------------------------------------------------------------------------------------------------------------------------------

#FOR PnL


INSERT_PNL_CLIENT_STG = text("""
INSERT INTO pnl_client_stg (tenant_id, row_hash, date, section, label, amount, issummary, accountid) 
VALUES (:tenant_id, :row_hash, :date, :section, :label, :amount, :issummary, :accountid)
""")

def load_PNL(from_date: str, to_date: str, period: str | None = None, timeframe: int | None = None):

    df = trigger_pnl(from_date, to_date, period=period, timeframe=timeframe)
    rows = df.to_dict(orient="records")

    inserted = 0

    with SessionLocal.begin() as session:
        try:
            session.execute(INSERT_PNL_CLIENT_STG, rows)
            inserted = len(rows)

            session.execute(text("""
                INSERT INTO pnl_client(tenant_id, row_hash, date, section, label, amount, issummary, accountid)
                SELECT tenant_id, row_hash, date, section, label, amount, issummary, accountid
                FROM pnl_client_stg
                WHERE NOT EXISTS (
                SELECT 1 FROM pnl_client a WHERE a.row_hash = pnl_client_stg.row_hash);"""))

            # 3) Update existing rows in final from staging (match on row_hash)
            session.execute(text("""
                UPDATE pnl_client AS c
                SET
                    tenant_id   = s.tenant_id,
                    date       = s.date,
                    section    = s.section,
                    label      = s.label,
                    amount     = s.amount,
                    issummary  = s.issummary,
                    accountid  = s.accountid
                FROM pnl_client_stg AS s
                WHERE s.row_hash = c.row_hash;"""))
            session.execute(text("DELETE FROM pnl_client_stg"))

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted,df

#------------------------------------------------------------------------------------------------------------------------------------------

#FOR Journals


INSERT_JOURNALS_STG = text("""
INSERT INTO journalsrawstg (

    tenant_id, journallineid, referencenumber, journalid, journaldate,
    accountid, accountcode, accounttype, accountname,
    description, sourcetype, reference, netamount, grossamount,
    taxamount, taxtype, taxname, debit, credit, createddateutc,
    trackingcategoriescount,
    trackingcategory1_name, trackingcategory1_option,
    trackingcategory1_trackingcategoryid, trackingcategory1_trackingoptionid,
    trackingcategory2_name, trackingcategory2_option,
    trackingcategory2_trackingcategoryid, trackingcategory2_trackingoptionid
)
VALUES (
    :tenant_id, :journallineid, :referencenumber, :journalid, :journaldate,
    :accountid, :accountcode, :accounttype, :accountname,
    :description, :sourcetype, :reference, :netamount, :grossamount,
    :taxamount, :taxtype, :taxname, :debit, :credit, :createddateutc,
    :trackingcategoriescount,
    :trackingcategory1_name, :trackingcategory1_option,
    :trackingcategory1_trackingcategoryid, :trackingcategory1_trackingoptionid,
    :trackingcategory2_name, :trackingcategory2_option,
    :trackingcategory2_trackingcategoryid, :trackingcategory2_trackingoptionid
)
""")


def load_JOURNALS():

    df = trigger_journals()

    # All columns even when clients dont produce
    expected_cols = [
        "tenant_id","journallineid","referencenumber","journalid","journaldate",
        "accountid","accountcode","accounttype","accountname",
        "description","sourcetype","reference","netamount","grossamount",
        "taxamount","taxtype","taxname","debit","credit","createddateutc",
        "trackingcategoriescount",
        "trackingcategory1_name","trackingcategory1_option",
        "trackingcategory1_trackingcategoryid","trackingcategory1_trackingoptionid",
        "trackingcategory2_name","trackingcategory2_option",
        "trackingcategory2_trackingcategoryid","trackingcategory2_trackingoptionid",
    ]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None  # create the column so keys exist in dicts

    # For fullproof checks
    df = df.where(df.notnull(), None)

    rows = df[expected_cols].to_dict(orient="records")  # preserve consistent key set & order

    inserted = 0

    with SessionLocal.begin() as session:
        try:
            # 1) Insert into staging
            session.execute(INSERT_JOURNALS_STG, rows)
            inserted = len(rows)

            # 2) Insert new rows into final if journallineid does not exist
            session.execute(text("""
                INSERT INTO journalsraw (
                    tenant_id,
                    journallineid, referencenumber, journalid, journaldate,
                    accountid, accountcode, accounttype, accountname,
                    description, sourcetype, reference, netamount, grossamount,
                    taxamount, taxtype, taxname, debit, credit, createddateutc,
                    trackingcategoriescount,
                    trackingcategory1_name, trackingcategory1_option,
                    trackingcategory1_trackingcategoryid, trackingcategory1_trackingoptionid,
                    trackingcategory2_name, trackingcategory2_option,
                    trackingcategory2_trackingcategoryid, trackingcategory2_trackingoptionid
                )
                SELECT
                    tenant_id,
                    journallineid, referencenumber, journalid, journaldate,
                    accountid, accountcode, accounttype, accountname,
                    description, sourcetype, reference, netamount, grossamount,
                    taxamount, taxtype, taxname, debit, credit, createddateutc,
                    trackingcategoriescount,
                    trackingcategory1_name, trackingcategory1_option,
                    trackingcategory1_trackingcategoryid, trackingcategory1_trackingoptionid,
                    trackingcategory2_name, trackingcategory2_option,
                    trackingcategory2_trackingcategoryid, trackingcategory2_trackingoptionid
                FROM journalsrawstg
                WHERE NOT EXISTS (
                    SELECT 1 FROM journalsraw j WHERE j.journallineid = journalsrawstg.journallineid
                );
            """))

            # 3) Update matching rows (by journallineid)
            session.execute(text("""
                UPDATE journalsraw AS j
                SET
                    tenant_id    = s.tenant_id,
                    referencenumber = s.referencenumber,
                    journalid = s.journalid,
                    journaldate = s.journaldate,
                    accountid = s.accountid,
                    accountcode = s.accountcode,
                    accounttype = s.accounttype,
                    accountname = s.accountname,
                    description = s.description,
                    sourcetype = s.sourcetype,
                    reference = s.reference,
                    netamount = s.netamount,
                    grossamount = s.grossamount,
                    taxamount = s.taxamount,
                    taxtype = s.taxtype,
                    taxname = s.taxname,
                    debit = s.debit,
                    credit = s.credit,
                    createddateutc = s.createddateutc,
                    trackingcategoriescount = s.trackingcategoriescount,
                    trackingcategory1_name = s.trackingcategory1_name,
                    trackingcategory1_option = s.trackingcategory1_option,
                    trackingcategory1_trackingcategoryid = s.trackingcategory1_trackingcategoryid,
                    trackingcategory1_trackingoptionid = s.trackingcategory1_trackingoptionid,
                    trackingcategory2_name = s.trackingcategory2_name,
                    trackingcategory2_option = s.trackingcategory2_option,
                    trackingcategory2_trackingcategoryid = s.trackingcategory2_trackingcategoryid,
                    trackingcategory2_trackingoptionid = s.trackingcategory2_trackingoptionid
                FROM journalsrawstg AS s
                WHERE s.journallineid = j.journallineid;
            """))

            # 4) Clear staging
            session.execute(text("DELETE FROM journalsrawstg"))

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted, df

#------------------------------------------------------------------------------------------------------------------

# FOR Manual journals


INSERT_MANUALJOURNALS_STG = text("""
INSERT INTO manualjournalsstg (
    tenant_id,
    manualjournalid,
    status,
    description,
    date,
    updateddateutc,
    lineamounttypes,
    showoncashbasisreports,
    hasattachments
    )
    VALUES (
    :tenant_id,
    :manualjournalid,
    :status,
    :description,
    :date,
    :updateddateutc,
    :lineamounttypes,
    :showoncashbasisreports,
    :hasattachments
    )
    
    """)


def load_MANUALJOURNALS():

    df = trigger_manualjournals()
    rows = df.to_dict(orient="records")

    # GETS UPDATED ONCE STARTS INSERTING...
    inserted = 0

    # Uses transaction...
    with SessionLocal.begin() as session:
        try:
            stmt = INSERT_MANUALJOURNALS_STG
            session.execute(stmt, rows)
            inserted = len(rows)

            session.execute(text("""
            INSERT INTO manualjournalsraw(
                tenant_id,
                manualjournalid,
                status,
                description,
                date,
                updateddateutc,
                lineamounttypes,
                showoncashbasisreports,
                hasattachments
            )
            SELECT 
                tenant_id,
                manualjournalid,
                status,
                description,
                date,
                updateddateutc,
                lineamounttypes,
                showoncashbasisreports,
                hasattachments
            FROM manualjournalsstg
            WHERE NOT EXISTS(
                SELECT 1 FROM manualjournalsraw a
                WHERE a.manualjournalid = manualjournalsstg.manualjournalid
            );
            """))

            session.execute(text("""
            UPDATE manualjournalsraw AS c
            SET
                tenant_id               = s.tenant_id,
                status                 = s.status,
                description            = s.description,
                date                   = s.date,
                updateddateutc         = s.updateddateutc,
                lineamounttypes        = s.lineamounttypes,
                showoncashbasisreports = s.showoncashbasisreports,
                hasattachments         = s.hasattachments
            FROM manualjournalsstg AS s
            WHERE s.manualjournalid = c.manualjournalid;
            """))

            session.execute(text("DELETE FROM manualjournalsstg"))
            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted, df


    #---------------------------------------------------------------------------------------------------------------

    # FOR ACCOUNTS

INSERT_ACCOUNTS_STG = text("""
INSERT INTO accountsstg (
    tenant_id,
    accountid,
    code,
    name,
    status,
    type,
    taxtype,
    class,
    enablepaymentstoaccount,
    showinexpenseclaims,
    bankaccountnumber,
    bankaccounttype,
    currencycode,
    reportingcode,
    reportingcodename,
    hasattachments,
    addtowatchlist,
    updateddateutc,
    description,
    reportingname,
    systemaccount
)
VALUES (
    :tenant_id,
    :accountid,
    :code,
    :name,
    :status,
    :type,
    :taxtype,
    :class,
    :enablepaymentstoaccount,
    :showinexpenseclaims,
    :bankaccountnumber,
    :bankaccounttype,
    :currencycode,
    :reportingcode,
    :reportingcodename,
    :hasattachments,
    :addtowatchlist,
    :updateddateutc,
    :description,
    :reportingname,
    :systemaccount)""")


def load_ACCOUNTS():
    df = trigger_account()
    rows = df.to_dict(orient="records")

    inserted = 0

    with SessionLocal.begin() as session:
        try:
            stmt = INSERT_ACCOUNTS_STG
            session.execute(stmt, rows)
            inserted = len(rows)

            # Anything new is added on a CI level...
            session.execute(text("""
            INSERT INTO accountsraw (
                tenant_id,
                accountid,
                code,
                name,
                status,
                type,
                taxtype,
                class,
                enablepaymentstoaccount,
                showinexpenseclaims,
                bankaccountnumber,
                bankaccounttype,
                currencycode,
                reportingcode,
                reportingcodename,
                hasattachments,
                addtowatchlist,
                updateddateutc,
                description,
                reportingname,
                systemaccount
            )
            SELECT
                tenant_id,
                accountid,
                code,
                name,
                status,
                type,
                taxtype,
                class,
                enablepaymentstoaccount,
                showinexpenseclaims,
                bankaccountnumber,
                bankaccounttype,
                currencycode,
                reportingcode,
                reportingcodename,
                hasattachments,
                addtowatchlist,
                updateddateutc,
                description,
                reportingname,
                systemaccount
            FROM accountsstg
            WHERE NOT EXISTS (
                SELECT 1 FROM accountsraw a
                WHERE a.accountid = accountsstg.accountid
            );
            """))

            # Uploaded data is updated, if an account is deleted or something...
            session.execute(text("""
            UPDATE accountsraw AS c
            SET
                tenant_id               = s.tenant_id,
                code                    = s.code,
                name                    = s.name,
                status                  = s.status,
                type                    = s.type,
                taxtype                 = s.taxtype,
                class                   = s.class,
                enablepaymentstoaccount = s.enablepaymentstoaccount,
                showinexpenseclaims     = s.showinexpenseclaims,
                bankaccountnumber       = s.bankaccountnumber,
                bankaccounttype         = s.bankaccounttype,
                currencycode            = s.currencycode,
                reportingcode           = s.reportingcode,
                reportingcodename       = s.reportingcodename,
                hasattachments          = s.hasattachments,
                addtowatchlist          = s.addtowatchlist,
                updateddateutc          = s.updateddateutc,
                description             = s.description,
                reportingname           = s.reportingname,
                systemaccount           = s.systemaccount
            FROM accountsstg AS s
            WHERE s.accountid = c.accountid;
            """))


            session.execute(text("DELETE FROM accountsstg"))
            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted, df

    #----------------------------------------------------------------------------------------------------------------------------