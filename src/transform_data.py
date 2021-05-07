from clean_raw_data import clean_intakes, clean_outcomes

def create_paired_records(intake_file, outcome_file):
    """Takes CSV versions of intake and outcome files, matches the paired in & out records, returns a DataFrame

    Args:
        intake_file (CSV file): CSV file of animal shelter intake records
        outcome_file (CSV file): CSV file of animal shelter outcome records

    Returns:
        pandas.DataFrame: DataFrame of paired intake and outcome records for each animal at the animal shelter.
    """
    # create initial dataframes
    ins = pd.read_csv(intake_file, parse_dates=['datetime'])
    outs = pd.read_csv(outcome_file, parse_dates=['datetime', 'date-of-birth'])

    # calculate the order of each record for each animal
    unpaired_append = ins[['animal-id', 'datetime', 'intake-type']].append(outs[['animal-id', 'datetime', 'outcome-type']])
    unpaired_append.sort_values(by=['animal-id', 'datetime'], ascending=True, inplace=True)
    unpaired_append['record-number'] = unpaired_append.groupby(by='animal-id')['datetime'].rank(method='first')

    # remove records where first action is an outcome, without an intake
    first_outs = unpaired_append.loc[(unpaired_append['outcome-type'].notna()) & (temp_append['record-number'] == 1), ['animal-id', 'datetime']].copy()
    subsequent_outs = outs.drop(first_outs.index)

    # calculate the order of intakes and subsequent outcomes
    ins.sort_values(by=['animal-id', 'datetime'], ascending=True, inplace=True)
    ins['intake-number'] = ins.groupby(by='animal-id')['datetime'].rank(method='first')
    subsequent_outs.sort_values(by=['animal-id', 'datetime'], ascending=True, inplace=True)
    subsequent_outs['outcome-number'] = subsequent_outs.groupby(by='animal-id')['datetime'].rank(method='first')

    ins.rename(columns={'datetime':'intake-date'}, inplace=True)
    subsequent_outs.rename(columns={'datetime':'outcome-date'}, inplace=True)

    # create merge of paired intake and outcome records
    outcome_specific_cols = [
        'animal-id', 
        'outcome-date',
        'date-of-birth', 
        'outcome-type', 
        'outcome-subtype', 
        'sex-upon-outcome', 
        'outcome-number'
        ]

    ins_and_outs = pd.merge(
        left=ins,
        right=subsequent_outs[outcome_specific_cols],
        how='left',
        left_on=['animal-id', 'intake-number'],
        right_on=['animal-id', 'outcome-number'])

    ins_and_outs.drop(columns=['intake-number', 'outcome-number', 'age-upon-intake'], inplace=True)

    new_order = [
        'animal-id', 
        'name',
        'animal-type', 
        'breed', 
        'color', 
        'date-of-birth', 
        'intake-date', 
        'intake-type', 
        'intake-condition', 
        'sex-upon-intake', 
        'outcome-date', 
        'outcome-type', 
        'outcome-subtype', 
        'sex-upon-outcome'
        ]

    return ins_and_outs[new_order]

paired_records = create_paired_records(clean_intakes, clean_outcomes)

