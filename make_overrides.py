import pandas as pd
import datetime
from datetime import datetime, timedelta

"""
deal_id     cutoff_date

Alliant1	2022-03-27
Exigent38	2022-03-03
Exigent36	2022-01-01
Union_01	2021-11-29
Exigent34	2021-11-01
Pagaya_06	2021-11-01
Exigent32	2021-09-01
Exigent2	2019-01-01

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
        "validation_check_1",
        "validation_check_2",
        "validation_check_3",
    ]


def make_partial_month_proposed_overrides(cutover_date: str, report_date: str, deal_id: str):
    """
        cutover_date = "YYYY-MM-DD" # should be a day in the prior month to report_date
        report_date = "YYYY-MM-DD" # should be the first of the month
        Put the appropriate servicing files in "input" folder.
        Resulting proposed overrides is produced in the "output" folder.
        Example:  cutover_date = 2023-02-15, report_date = 2023-03-01
        "The ending balance on the cutover date is what gets sold"
        servicing_start_date == cutover date + 1 day
        Check the  "filename" date equals the report_date column the file: crb_originated_loans_report_<REPORT_DATE>.csv
    """

    servicing_start_date = (datetime.strptime(cutover_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
    servicing_start_filename = f'./input/crb_originated_loans_report_{servicing_start_date}.csv'

    servicing_end_date = datetime.strptime(report_date, '%Y-%m-%d').strftime('%Y%m%d')
    servicing_end_filename = f'./input/crb_originated_loans_report_{servicing_end_date}.csv'
    output_filename = f'./output/participations_data_quality_checks_{servicing_end_date}_approved.csv'

    servicing_start = pd.read_csv(servicing_start_filename, dtype=str, index_col='mpl_acct_id')
    servicing_end = pd.read_csv(servicing_end_filename, dtype=str, index_col='mpl_acct_id')

    participated_loans = pd.read_csv('./input/participated_loans.csv', dtype=str, index_col='mpl_acct_id')
    participated_loans = participated_loans[participated_loans['Deal ID'] == deal_id]
    proposed_overrides = pd.DataFrame(columns=PROPOSED_OVERRIDES_COLUMNS, index=participated_loans.index)
    proposed_overrides.index.name = 'mpl_acct_id'
    # filter down servicing data to just the loans of interest
    servicing_start = servicing_start.loc[participated_loans.index]
    servicing_end = servicing_end.loc[participated_loans.index]

    numeric_cols = [
        "principal_pmt",
        "interest_pmt",
        "payment_recoveries",
        "fraud_amt",
        "charged_off_principal",
        "charged_off_interest",
        "simple_interest_accrued",
        "simple_interest_accrued_last_month",
        "ending_balance",
        "days_past_due",
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

    # IN THIS SCRIPT "TAPE" IS FROM THE INVESTORS PERSPECTIVE (NOT FROM A "PURE" SERVICING FILE PERSPECTIVE)
    proposed_overrides["originator_loan_id"] = proposed_overrides.index
    proposed_overrides["investor"] = participated_loans['Investor']
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
    proposed_overrides["prior_loan_status"] = servicing_start['loan_status']
    proposed_overrides["cur_days_past_due"] = servicing_end['days_past_due']
    proposed_overrides["prior_days_past_due"] = servicing_start['days_past_due']
    proposed_overrides["cur_monthly_pmt_amt"] = servicing_end['monthly_pmt_amt']
    proposed_overrides["cur_borrower_refunds"] = None
    proposed_overrides["cur_charged_off_principal"] = servicing_end['charged_off_principal'] - servicing_start['charged_off_principal']
    proposed_overrides["cur_principal_pmt"] = servicing_end['principal_pmt'] - servicing_start['principal_pmt']
    proposed_overrides["cur_interest_pmt"] = servicing_end['interest_pmt'] - servicing_start['interest_pmt']
    # proposed
    proposed_overrides["cur_rpt_beg_bal_proposed"] = servicing_start['ending_balance']
    proposed_overrides["cur_rpt_charged_off_principal_proposed"] = proposed_overrides["cur_charged_off_principal"]
    proposed_overrides["cur_rpt_interest_pmt_proposed"] = proposed_overrides["cur_interest_pmt"]
    proposed_overrides["cur_rpt_borrower_refunds_proposed"] = 0
    proposed_overrides["cur_rpt_end_bal_proposed"] = servicing_end['ending_balance']
    proposed_overrides["cur_rpt_principal_pmt_proposed"] = proposed_overrides["cur_principal_pmt"]
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
    proposed_overrides['cur_rpt_accrued_interest_last_month_proposed'] = proposed_overrides['cur_rpt_accrued_interest_last_month']
    proposed_overrides['cur_rpt_accrued_interest_proposed'] = proposed_overrides['cur_rpt_accrued_interest']
    proposed_overrides['cur_rpt_payment_recoveries_proposed'] = proposed_overrides['cur_rpt_payment_recoveries']
    proposed_overrides['cur_rpt_servicing_fees_proposed'] = 0
    proposed_overrides['cur_rpt_borrower_fees_proposed'] = 0
    proposed_overrides['cur_rpt_agency_fees_proposed'] = 0
    proposed_overrides['cur_rpt_agency_fails_proposed'] = 0 
    proposed_overrides['cur_rpt_fraud_amount_proposed'] = proposed_overrides['cur_rpt_fraud_amount']
    proposed_overrides['cur_rpt_co_loan_sale_proposed'] = 0
    proposed_overrides['cur_rpt_late_fee_paid_proposed'] = 0
    proposed_overrides['cur_rpt_nsf_paid_proposed'] = 0
    proposed_overrides['cur_rpt_wrong_debt_settlement_ach_proposed'] = 0
    proposed_overrides['cur_rpt_misc_costs_and_fees_proposed'] = 0
    # Rule to signal data issue
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Other' if 0 != round(x.cur_rolled_end_balance - x.cur_tape_end_balance, 2) else None, axis=1)
    # order matters because we overwrite comments
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Borrower overpayment 1st time, pending refund' if (x.cur_tape_end_balance == 0) & (x.cur_rolled_end_balance < 0) else x.comment, axis=1)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Return Payment, Delinquent' if (x.cur_days_past_due > 0) & (x.prior_days_past_due == 0) & (x.prior_loan_status.lower() == 'current') & (x.cur_loan_status.lower() != 'current') & (x.cur_tape_end_balance > x.cur_tape_beg_balance) else x.comment, axis=1)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Return Payment, Delinquent but Loan Status is Current' if (x.cur_days_past_due > 0) & (x.prior_days_past_due == 0) & (x.prior_loan_status.lower() == 'current') & (x.cur_loan_status.lower() == 'current') & (x.cur_tape_end_balance > x.cur_tape_beg_balance) else x.comment, axis=1)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Return Payment, Not Marked Delinquent or DPD' if (x.prior_days_past_due == 0) & (x.prior_loan_status.lower() == 'current') & (x.cur_loan_status.lower() == 'current') & (x.cur_tape_end_balance > x.cur_tape_beg_balance) else x.comment, axis=1)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Return Payment, Current throughout and end tape was greater than beg tape balance' if (x.cur_tape_beg_balance == x.prior_tape_end_balance) & (x.cur_tape_end_balance > x.cur_tape_beg_balance) & (x.prior_loan_status.lower() == 'current') & (x.cur_loan_status.lower() == 'current') else x.comment, axis=1)
    proposed_overrides['comment'] = proposed_overrides.apply(lambda x: 'Payment received, not reflected on tape' if (x.cur_tape_beg_balance < x.prior_tape_end_balance) & (x.prior_tape_end_balance > 0) else x.comment, axis=1)

    proposed_overrides.to_csv(output_filename, index=False)


if __name__ == '__main__':
    cutover_date="2022-03-03"
    report_date="2022-04-01"
    deal_id='Exigent #38'
    make_partial_month_proposed_overrides(cutover_date, report_date, deal_id)
