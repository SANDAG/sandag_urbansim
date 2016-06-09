import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
from urbansim.utils import yamlio
import numpy as np

yaml_outfile = "../configs/rsh_luz_only.yaml"

# create yaml from model stored in database
# database -> python dictionary -> yaml

# static header info
dict_luz_rsh = {'name': 'rsh',
                'model_type': 'segmented_regression',
                'segmentation_col': 'building_type_id',
                'fit_filters': ['res_price_per_sqft > 0',
                                'building_type_id in [19,20,21]',
                                'residential_units > 0',
                                'year_built > 1000',
                                'year_built < 2020'],
                'predict_filters': ['residential_units > 0',
                                    'building_type_id in [19]'],
                'min_segment_size': 10
                }

# SAS regression model from database for building_type_id = 19
regression_engine = create_engine(get_connection_string("../configs/dbconfig.yml", 'regression_database'))
luz_sql = 'SELECT * FROM dbo.regression_data_luz_1'
luz_df = pd.read_sql(luz_sql, regression_engine, index_col='parcel_id')

db_first_row = luz_df.head(1) # contains all coefficients needed for model
db_sas_dict = db_first_row.to_dict()

# coefficients to dictionary
coefficient_keys = [column_name for column_name, coefficient_value in db_sas_dict.items() if column_name.startswith('p_luz')]
coefficients = {p_luz: db_sas_dict[p_luz] for p_luz in coefficient_keys}

# standard error to dictionary
se_keys = [column_name for column_name, std_error_value in db_sas_dict.items() if column_name.startswith('se_luz')]
se = {se_luz: db_sas_dict[se_luz] for se_luz in se_keys}

# T-score to dictionary
t_score_keys = [column_name for column_name, t_score_value in db_sas_dict.items() if column_name.startswith('t_luz')]
t_score = {t_luz: db_sas_dict[t_luz] for t_luz in t_score_keys}


# Format data for model specification in yaml
# e.g. I(luz_id == 60)[T.True]: -0.31077807116719014
# note: must have white space around equal sign
# note: value from db dictionary is numpy float, convert to python float
# before writing to yaml, otherwise odd results


# coefficients
param_dict = dict()
fit_params = dict()
for key, value in coefficients.items():
        luz_id = int(key[-3:])  # e.g. 009 from p_luz_009
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]  # coefficient
        value_from_db_float = np.float64(value_from_db_np).item()
        # type(value_from_db_np);  type(value_from_db_float)
        param_dict[key_name] = value_from_db_float
param_dict['Intercept'] = np.float64(db_sas_dict['p_Intercept'].values()[0]).item()
fit_params['Coefficient'] = param_dict

# standard error
se_dict = dict()
for key, value in se.items():
        luz_id = int(key[-3:]) #
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        se_dict[key_name] = value_from_db_float
se_dict['Intercept'] = np.float64(db_sas_dict['se_Intercept'].values()[0]).item()
fit_params['Std. Error'] = se_dict

# T score
t_dict = dict()
for key, value in t_score.items():
        luz_id = int(key[-3:])
        key_name = "I(luz_id == " + str(luz_id) + ")[T.True]"
        value_from_db_np = value.values()[0]
        value_from_db_float = np.float64(value_from_db_np).item()
        t_dict[key_name] = value_from_db_float
t_dict['Intercept'] = np.float64(db_sas_dict['t_Intercept'].values()[0]).item()
fit_params['T-Score'] = t_dict

# model expression
expr = 'np.log1p(res_price_per_sqft) ~ '
# no plus sign for the first variable
for key, value in coefficients.items()[0:1]:
    luz_id = int(key[-3:])
    equation_var = ' I(luz_id==' + str(luz_id) + ')'
    expr += equation_var
# use plus sign for the rest of the equation
for key, value in coefficients.items()[1:]:
    luz_id = int(key[-3:])
    equation_var = ' + I(luz_id==' + str(luz_id) + ')'
    expr += equation_var

# for building_type_id = 19
dict_luz_rsh['models'] = {}
dict_luz_rsh['models'][19L] = {}
dict_luz_rsh['models'][19L]['name'] = 19L
dict_luz_rsh['models'][19L]['fit_parameters'] = fit_params
dict_luz_rsh['models'][19L] ['model_expression'] = expr
dict_luz_rsh['models'][19L]['fitted'] = True
dict_luz_rsh['models'][19L]['fit_rsquared'] = 0.26386213899155664
dict_luz_rsh['models'][19L]['fit_rsquared_adj'] = 0.2621440640086523
dict_luz_rsh['default_config'] = {}
# dict_luz_rsh['default_config']['model_expression'] = expr
# dict_luz_rsh['default_config']['ytransform'] = 'np.exp'


yamlio.convert_to_yaml(dict_luz_rsh,yaml_outfile)
