import pandas as pd

"""
    This would flag an error:
        cur_rolled_end_balance <> cur_tape_end_balance

        cur rolled end balance <> cur tape end balance
            from dbt: (b.outstanding_principal_bop - b.principal_received - abs(b.charged_off_principal)) as rolled_end_balance
            form k's notes: Rolled end balance is different, adjusted for mid-month sale new rolled end = end_bal_4_15 - (princ_pmt_5_1 - princ_pmt_4_15)

    'Borrower overpayment 1st time, pending refund'
    (cur_tape_end_balance = 0) AND (cur_rolled_end_balance < 0) AND  (prior_rpt_end_balance > 0) AND (cur_borrower_refunds = 0) AND (prior_rpt_end_balance = prior_tape_end_balance)

    'Return Payment, Delinquent'
    (cur_tape_beg_balance > prior_tape_end_balance) AND (cur_days_past_due > 0) AND  (prior_days_past_due = 0) AND (prior_loan_status = 'Current') AND (cur_loan_status <> 'Current')
        •	'Return Payment, Delinquent but Loan Status is Current'
    (cur_tape_beg_balance > prior_tape_end_balance) AND (cur_days_past_due > 0) AND (prior_days_past_due = 0) AND (prior_loan_status = 'Current') AND (cur_loan_status = 'Current')
        •	'Return Payment, Not Marked Delinquent or DPD'
    (cur_tape_beg_balance > prior_tape_end_balance) AND (prior_days_past_due = 0) AND (prior_loan_status = 'Current') AND (cur_loan_status = 'Current')
        •	'Return Payment, Current throughout and end tape was greater than beg tape balance'
    (cur_tape_beg_balance = prior_tape_end_balance) AND (cur_tape_end_balance > cur_tape_beg_balance) AND (prior_loan_status = 'Current') AND (cur_loan_status = 'Current')

        •	'Payment received, not reflected on tape'
    (prior_tape_end_balance = prior_rpt_end_balance) AND (cur_tape_beg_balance < prior_tape_end_balance)   AND (prior_tape_end_balance > 0)
        •	'Other'
"""


"""
    SUM(COALESCE(a.principal_received,0)) as principal_received,
    SUM(COALESCE(a.interest_received,0)) as interest_received,
    SUM(COALESCE(a.payment_recoveries,0)) as payment_recoveries,
    -SUM(COALESCE(a.servicing_fees,0)) as servicing_fees,
    -SUM(COALESCE(a.borrower_fees,0)) as borrower_fees,
    -SUM(COALESCE(a.borrower_refunds,0)) as borrower_refunds,
    -SUM(COALESCE(a.agency_fees,0)) as agency_fees,
    -SUM(COALESCE(a.agency_fails,0)) as agency_fails,
    -SUM(COALESCE(a.charged_off_principal,0)) as charged_off_principal,
    SUM(COALESCE(a.fraud_amount,0)) as fraud_amount,
    SUM(COALESCE(a.outstanding_principal_bop,0)) as outstanding_principal_bop,
    SUM(COALESCE(a.accrued_interest,0)) as accrued_interest,
    SUM(COALESCE(a.accrued_interest_last_month,0)) as accrued_interest_last_month,
    SUM(COALESCE(a.outstanding_principal_eop)) as outstanding_principal_eop,
    SUM(COALESCE(a.co_loan_sale, 0)) as co_loan_sale,
    SUM(COALESCE(a.late_fee_paid, 0)) as late_fee_paid,
    SUM(COALESCE(a.nsf_paid, 0)) as nsf_paid,
    MAX(a.servicing_fee_pct) servicing_fee_pct_max,
    MIN(a.servicing_fee_pct) servicing_fee_pct_min,
"""

PROPOSED_OVERRIDES_COLUMNS = [
        "originator_loan_id",
        "investor",
        "cur_report_date",
        "cur_rolled_end_balance",
        "cur_tape_end_balance",
        "cur_tape_beg_balance",
        "prior_tape_end_balance",
        "prior_rolled_end_balance",
        "prior_rpt_end_balance",
        "cur_loan_status",
        "prior_loan_status",
        "cur_days_past_due",
        "prior_days_past_due",
        "cur_monthly_pmt_amt",
        "cur_borrower_refunds",
        "cur_charged_off_principal",
        "cur_principal_pmt",
        "cur_interest_pmt",
        "rule_1",
        "rule_2",
        "rule_3",
        "rule_4",
        "comment",
        "cur_rpt_beg_bal_proposed",
        "cur_rpt_charged_off_principal_proposed",
        "cur_rpt_interest_pmt_proposed",
        "cur_rpt_borrower_refunds_proposed",
        "cur_rpt_end_bal_proposed",
        "cur_rpt_principal_pmt_proposed",
        "val_1_a",
        "val_1_b",
        "VALIDATION_CHECK_1",
        "VALIDATION_CHECK_2",
        "VALIDATION_CHECK_3",
        # new
    ]


def make_partial_month_proposed_overrides(investor='Alliant'):
    """
    -- cutover 3/27
    --- report date 3/28 ending balance
    --- "The ending balance of the cutover date is what gets sold (note report date = cutover date + 1)"
    -- add a check that confirm that "filename" date equals the report_date column the file
    """
    cutoff_date = '2023-02-14'
    servicing_start = pd.read_csv('./input/crb_originated_loans_report_20230215.csv', dtype=str, index_col='mpl_acct_id')
    servicing_end = pd.read_csv('./input/crb_originated_loans_report_20230301.csv', dtype=str, index_col='mpl_acct_id')
 
    participated_loans = pd.read_csv('./input/participated_loans.csv', dtype=str, index_col='mpl_acct_id')
    participated_loans = participated_loans[participated_loans['Investor'] == 'Alliant']
    # servicing_start = servicing_start.loc[participated_loans.index]
    # servicing_end = servicing_end.loc[participated_loans.index]
    proposed_overrides = pd.DataFrame(columns=PROPOSED_OVERRIDES_COLUMNS, index=participated_loans.index)
    proposed_overrides.index.name = 'mpl_acct_id'
    numeric_cols = [
        "principal_pmt",
        "interest_pmt",
        "payment_recoveries",
        "fraud_amt",
        "charged_off_principal",
        "charged_off_interest",
        "simple_interest_accrued",
        "simple_interest_accrued_last_month",
        "ending_balance"
    ]
    servicing_start[numeric_cols] = servicing_start[numeric_cols].apply(pd.to_numeric).fillna(0)
    servicing_end[numeric_cols] = servicing_end[numeric_cols].apply(pd.to_numeric).fillna(0)

    empty_columns = [
        "rule_1",
        "rule_2",
        "rule_3",
        "rule_4",
        "comment",
        "val_1_a",
        "val_1_b",
        "VALIDATION_CHECK_1",
        "VALIDATION_CHECK_2",
        "VALIDATION_CHECK_3",
    ]

    # IN THIS SCRIPT "TAPE" IS FROM THE INVESTORS PERSPECTIVE (NOT FROM A PURE SERVICING FILE PERSPECTIVE)
    proposed_overrides["originator_loan_id"] = proposed_overrides.index
    proposed_overrides["investor"] = 'Alliant'
    proposed_overrides["cur_report_date"] = '2023-03-01'
    proposed_overrides["cur_rolled_end_balance"] = (
                  servicing_start['ending_balance'] 
                - (servicing_end['principal_pmt'] - servicing_start['principal_pmt'])
                - (abs(servicing_end['charged_off_principal']) - abs(servicing_start['charged_off_principal']))
                )
    proposed_overrides["cur_tape_end_balance"] = servicing_end['ending_balance']
    proposed_overrides["cur_tape_beg_balance"] = servicing_start['ending_balance']
    proposed_overrides["prior_tape_end_balance"] = servicing_start['ending_balance']  # so we don't have to pull in the day before
    proposed_overrides["prior_rolled_end_balance"] = None
    proposed_overrides["prior_rpt_end_balance"] = servicing_start['beginning_balance']
    proposed_overrides["cur_loan_status"] = servicing_end['loan_status']
    # should this be the day before the cutover date or hardcoded as 'current'?
    # check with jason
    proposed_overrides["prior_loan_status"] = servicing_start['loan_status']
    proposed_overrides["cur_days_past_due"] = servicing_end['days_past_due']
    # should this be the day before the cutover date or hardcoded as 0?
    proposed_overrides["prior_days_past_due"] = servicing_start['days_past_due']
    proposed_overrides["cur_monthly_pmt_amt"] = servicing_end['monthly_pmt_amt']
    proposed_overrides["cur_borrower_refunds"] = None
    proposed_overrides["cur_charged_off_principal"] = servicing_end['charged_off_principal'] - servicing_start['charged_off_principal']
    proposed_overrides["cur_principal_pmt"] = servicing_end['principal_pmt'] - servicing_start['principal_pmt']
    proposed_overrides["cur_interest_pmt"] = servicing_end['interest_pmt'] - servicing_start['interest_pmt']
    # proposed
    proposed_overrides["cur_rpt_beg_bal_proposed"] = None
    proposed_overrides["cur_rpt_charged_off_principal_proposed"] = None
    proposed_overrides["cur_rpt_interest_pmt_proposed"] = None
    proposed_overrides["cur_rpt_borrower_refunds_proposed"] = 0
    proposed_overrides["cur_rpt_end_bal_proposed"] = servicing_end['ending_balance']
    proposed_overrides["cur_rpt_principal_pmt_proposed"] = None
    ## new cur_rpt fields
    proposed_overrides['cur_rpt_accrued_interest_last_month'] = 0
    proposed_overrides['cur_rpt_accrued_interest'] = servicing_end['simple_interest_accrued'] - servicing_start['simple_interest_accrued']
    proposed_overrides['cur_rpt_payment_recoveries'] = servicing_end['payment_recoveries'] - servicing_start['payment_recoveries']
    proposed_overrides['cur_rpt_servicing_fees'] = 0 
    proposed_overrides['cur_rpt_borrower_fees'] = 0 
    proposed_overrides['cur_rpt_agency_fees'] = 0 
    proposed_overrides['cur_rpt_agency_fails'] = 0  
    proposed_overrides['cur_rpt_fraud_amount'] = servicing_end['fraud_amt'] - servicing_start['fraud_amt']
    proposed_overrides['cur_rpt_co_loan_sale'] = 0 
    proposed_overrides['cur_rpt_late_fee_paid'] = 0 
    proposed_overrides['cur_rpt_nsf_paid'] = 0 
    proposed_overrides['cur_rpt_wrong_debt_settlement_ach'] = 0 
    proposed_overrides['cur_rpt_misc_costs_and_fees'] = 0 
    ## new proposed fields
    proposed_overrides['cur_rpt_accrued_interest_last_month_proposed'] = None
    proposed_overrides['cur_rpt_accrued_interest_proposed'] = None
    proposed_overrides['cur_rpt_payment_recoveries_proposed'] = None
    proposed_overrides['cur_rpt_servicing_fees_proposed'] = 0
    proposed_overrides['cur_rpt_borrower_fees_proposed'] = 0
    proposed_overrides['cur_rpt_agency_fees_proposed'] = 0
    proposed_overrides['cur_rpt_agency_fails_proposed'] = 0 
    proposed_overrides['cur_rpt_fraud_amount_proposed'] = None
    proposed_overrides['cur_rpt_co_loan_sale_proposed'] = 0
    proposed_overrides['cur_rpt_late_fee_paid_proposed'] = 0
    proposed_overrides['cur_rpt_nsf_paid_proposed'] = 0
    proposed_overrides['cur_rpt_wrong_debt_settlement_ach_proposed'] = 0
    proposed_overrides['cur_rpt_misc_costs_and_fees_proposed'] = 0
    # proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'error' if x.cur_rolled_end_balance != x.cur_tape_end_balance else None)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: round(x.cur_rolled_end_balance -  x.cur_tape_end_balance, 2), axis=1)


    proposed_overrides.to_csv('./output/partial_month_proposed_overrides.csv')


if __name__ == '__main__':
    make_partial_month_proposed_overrides()