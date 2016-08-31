import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
from urbansim.utils import yamlio
import numpy as np
import time

create_date = time.strftime("%Y%m%d")
yaml_outfile = 'configs/rsh_' + create_date + '.yaml'
dev_types = [19,20,21] # residential development ids

# parameters for model stored in database
coefficient_db = {}
coefficient_db[19] = 'input.regression_devtype19_20'
coefficient_db[20] = 'input.regression_devtype19_20'
coefficient_db[21] = 'input.regression_devtype21'

# for segmenting on dev type in yaml
dev = {}
dev[19] = 19L
dev[20] = 20L
dev[21] = 21L

# parameters based on database (not LUZ ids)
# dict w parameter name in db as key and parameter name in yaml as value
parameters = {}
parameters[19] = {}
parameters[19]['p_distance_to_coast2'] = 'I(distance_to_coast_mi)'
parameters[19]['p_distance_to_transit2'] = 'I(distance_to_transit_mi)'
parameters[19]['p_distance_to_onramp2'] = 'I(distance_to_onramp_mi)'
parameters[19]['p_nlog_ave_income'] = 'I(ave_income)'
parameters[19]['p_structure_age'] = 'I(structure_age)'
parameters[19]['p_nlog_parcel_sqft'] = 'I(np.log(parcel_size))'
parameters[19]['p_jobs_5000ft'] = 'I(jobs_5000ft)'
# parameters[19]['p_school_lthalfmi'] = 'I(distance_to_school < 2640)'
parameters[19]['p_year_built_lt1950'] = 'I(year_built < 1950)'
parameters[19]['p_year_built_lt1960'] = 'I(year_built < 1960)'
parameters[19]['p_year_built_lt2010'] = 'I(year_built > 2010)'

parameters[20] = {}
parameters[20]['p_distance_to_coast2'] = 'I(distance_to_coast_mi)'
parameters[20]['p_distance_to_transit2'] = 'I(distance_to_transit_mi)'
parameters[20]['p_distance_to_onramp2'] = 'I(distance_to_onramp_mi)'
parameters[20]['p_nlog_ave_income'] = 'I(ave_income)'
parameters[20]['p_structure_age'] = 'I(structure_age)'
parameters[20]['p_nlog_parcel_sqft'] = 'I(np.log(parcel_size))'
parameters[20]['p_jobs_5000ft'] = 'I(jobs_5000ft)'
# parameters[20]['p_school_lthalfmi'] = 'I(distance_to_school < 2640)'
parameters[20]['p_year_built_lt1950'] = 'I(year_built < 1950)'
parameters[20]['p_year_built_lt1960'] = 'I(year_built < 1960)'
parameters[20]['p_year_built_lt2010'] = 'I(year_built > 2010)'

parameters[21] = {}
parameters[21]['p_school_lthalfmi'] = 'I(distance_to_school < 2640)'
parameters[21]['p_onramp_lthalfmi'] = 'I(distance_to_onramp < 2640)'
parameters[21]['p_year_built_lt2010'] = 'I(year_built > 2010)'
parameters[21]['p_freeway_ltonemi'] = 'I(distance_to_freeway < 5280)'
parameters[21]['p_park_lthalfmi'] = 'I(distance_to_park < 2640)'
parameters[21]['p_distance_to_coast2'] = 'I(distance_to_coast_mi)'
parameters[21]['p_distance_to_transit2'] = 'I(distance_to_transit_mi)'
parameters[21]['p_jobs_5000ft'] = 'I(jobs_5000ft)'

# initialize header in yaml
rsh_luz = {}
rsh_luz['models'] = {}
rsh_luz['name'] = 'rsh'
rsh_luz['model_type'] = 'segmented_regression'
rsh_luz['segmentation_col'] = 'building_type_id'
rsh_luz['fit_filters'] = ['res_price_per_sqft > 0',
                          'building_type_id in [19,20,21]',
                          'residential_units > 0',
                          'year_built > 1000',
                          'year_built < 2020']
rsh_luz['predict_filters'] = ['residential_units > 0',
                              'building_type_id in [19,20,21]',
                              'year_built > 0']
rsh_luz['min_segment_size'] = 10


# connect to db with regression model
regression_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'regression_database'))

for dev_type_id in dev_types:
    luz_sql = 'SELECT TOP 10 * FROM ' + coefficient_db[dev_type_id]
    luz_df = pd.read_sql(luz_sql, regression_engine, index_col='parcel_id')
    db_first_row = luz_df.head(1) # contains all coefficients needed for model
    db_parms = db_first_row.to_dict()

    # create dict with luz coefficients (starts w. p_luz)
    coefficient_keys_luz = [column_name for column_name, coefficient_value in db_parms.items() if column_name.startswith('p_luz')]
    coefficients_luz = {p_luz: db_parms[p_luz] for p_luz in coefficient_keys_luz}

    # create dictionary with other coefficients based on parameters
    coefficient_keys_other = parameters[ dev_type_id].keys()
    coefficients_other = {parm: db_parms[parm] for parm in coefficient_keys_other}

    # create dictionary with standard error coefficients
    se_keys = [column_name for column_name, std_error_value in db_parms.items() if column_name.startswith('se_luz')]
    se = {se_luz: db_parms[se_luz] for se_luz in se_keys}

    # create dictionary with t-score
    t_score_keys = [column_name for column_name, t_score_value in db_parms.items() if column_name.startswith('t_luz')]
    t_score = {t_luz: db_parms[t_luz] for t_luz in t_score_keys}

    fit_params = {}
    param_dict = {}

    for key, value in coefficients_other.items():
        parameter_name = parameters[dev_type_id][key]
        comparison_operators = ['>','<','==','>=','<=']
        if any(operator in parameter_name for operator in comparison_operators):
            key_name = parameter_name + "[T.True]"
        else:
            key_name = parameter_name
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        param_dict[key_name] = value_from_db_float

    for key, value in coefficients_luz.items():
        luz_id = int(key[-3:])
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        param_dict[key_name] = value_from_db_float

    param_dict['Intercept'] = np.float64(db_parms['p_Intercept'].values()[0]).item()
    fit_params['Coefficient'] = param_dict

    se_dict = {}
    for key, value in se.items():
        luz_id = int(key[-3:])
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        se_dict[key_name] = value_from_db_float
    se_dict['Intercept'] = np.float64(db_parms['se_Intercept'].values()[0]).item()
    fit_params['Std. Error'] = se_dict
    # t score
    t_dict = {}
    for key, value in t_score.items():
        luz_id = int(key[-3:])
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        t_dict[key_name] = value_from_db_float
    t_dict['Intercept'] = np.float64(db_parms['t_Intercept'].values()[0]).item()
    fit_params['T-Score'] = t_dict

    expr = 'np.log1p(res_price_per_sqft) ~ '

    for key, value in parameters[dev_type_id].items():
        expr += value + ' + '

    expr = expr[:-2] # remove trailing plus sign

    for key, value in coefficients_luz.items()[0:]:
        luz_id = int(key[-3:])
        equation_var = ' + I(luz_id==' + str(luz_id) + ')'
        expr += equation_var

    rsh_luz['models'][dev[dev_type_id]] = {}
    rsh_luz['models'][dev[dev_type_id]]['fit_parameters'] = fit_params
    rsh_luz['models'][dev[dev_type_id]] ['model_expression'] = expr
    rsh_luz['models'][dev[dev_type_id]]['fitted'] = True
    rsh_luz['models'][dev[dev_type_id]]['fit_rsquared'] = 0.264
    rsh_luz['models'][dev[dev_type_id]]['fit_rsquared_adj'] = 0.262
    rsh_luz['default_config'] = {}
    rsh_luz['default_config']['model_expression'] = expr
    rsh_luz['default_config']['ytransform'] = 'np.exp'
    rsh_luz['models'][dev[dev_type_id]]['name'] = dev[dev_type_id]

yamlio.convert_to_yaml(rsh_luz,yaml_outfile)

