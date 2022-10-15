import os

os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = "USA Prescriber Research Report"
# current_path = os.getcwd()
current_path = os.path.normpath(os.getcwd() + os.sep + os.pardir)
staging_dim_city = current_path + '/staging/dimension_city'
staging_fact = current_path + '/staging/fact'

