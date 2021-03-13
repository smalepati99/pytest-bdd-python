import pytest

from pytest_bdd import scenario, given, when, then


@given('the SASfile is in Source AWS S3 "<sourceBucket>" with <SASfileName>')
def step_impl():
    raise NotImplementedError(u'STEP: Given the SASfile is in Source AWS S3 "<sourceBucket>" with <SASfileName>')


@when("I run the Gluejob with <jobName> to transfer the file")
def step_impl():
    raise NotImplementedError(u'STEP: When I run the Gluejob with <jobName> to transfer the file')


@given("the Gluejob <jobName> is sucessfully run")
def step_impl():
    raise NotImplementedError(u'STEP: And the Gluejob <jobName> is sucessfully run')


@then('I should see the <parquetFileName> is in the <targetbucketName>" in parquet Format')
def step_impl():
    raise NotImplementedError(
        u'STEP: Then I should see the <parquetFileName> is in the <targetbucketName>" in parquet Format')


@given("the SASfile that is transferred in the target parquet AWS S3 bucket")
def step_impl():
    raise NotImplementedError(u'STEP: Given the SASfile that is transferred in the target parquet AWS S3 bucket')


@when("I download the file for verification")
def step_impl():
    raise NotImplementedError(u'STEP: When I download the file for verification')


@then("the file should be downloaded in parquet Format")
def step_impl():
    raise NotImplementedError(u'STEP: Then the file should be downloaded in parquet Format')


@given("the file name pattern along with the business date, daily, weekly run info is as expected")
def step_impl():
    raise NotImplementedError(
        u'STEP: And  the file name pattern along with the business date, daily, weekly run info is as expected')


@given("the <auditcolumns> are added are as expected")
def step_impl():
    raise NotImplementedError(u'STEP: And  the <auditcolumns> are added are as expected')


@given("the values in the <auditcolumns> are correct")
def step_impl():
    raise NotImplementedError(u'STEP: And the values in the <auditcolumns> are correct')

#
# @pytest.fixture
# def df():
#     schema = StructType(
#           [
#             StructField(name="TAX_CODE", dataType=StringType(), nullable=False),
#             StructField(name="TAX_SLAB_INC88", dataType=FloatType(), nullable=False),
#             StructField(name="TAX_SLAB_TAX88", dataType=FloatType(), nullable=False),
#             StructField(name="TAXSLAB_INC89", dataType=FloatType(), nullable=False),
#             StructField(name="TAXSLAB_TAX89", dataType=FloatType(), nullable=False),
#           ]
#         )
#     rows = [
#             # TAX_CODE       TAX_SLAB_INC88	        TAX_SLAB_TAX88	   TAXSLAB_INC89	 TAXSLAB_TAX89
#             Row('BAKU',   9.21500015258789,     1.6430000066757202, 9.517999649047852,  2.125),
#             Row('NGNK',   2.046999931335449,	0.4129999876022339,	2.068000078201294,	0.5649999976158142),
#             Row('KKGX',   9.98900032043457,	    1.7519999742507935,	9.991999626159668,	2.2209999561309814),
#             Row('MBUK',   8.321000099182129,	1.4079999923706055,	8.515000343322754,	1.9049999713897705),
#             Row('NGUK',   4.5879998207092285,	0.8379999995231628,	4.388999938964844,	0.9430000185966492),
#             Row('MVBG',   4.736000061035156,	0.7480000257492065,	5.014999866485596,	1.0509999990463257),
#             Row('MKUV',   3.5959999561309814,	0.5770000219345093,	3.811000108718872,	0.8190000057220459),
#             Row('WXYG',   4.829999923706055,	0.7519999742507935,	4.939000129699707,	1.0149999856948853),
#             Row('VXGH',   4.507999897003174,	0.7609999775886536,	4.539000034332275,	1.0959999561309814),
#     ]
#     parellizeRows = spark.sparkContext.parallelize(rows)
#     df = spark.createDataFrame(parellizeRows, schema)
#     df.show()
#     return df
# import org.apache.spark.sql.types.IntegerType
# @given("I verify the data type checks are as expected")
# def step_impl()
#
#     (when dict(df.dtypes)['TAX_CODE'] == 'StringType')
#             dict(df.dtypes)['TAX_SLAB_TAX88'] == 'FloatType'
#             dict(df.dtypes)['TAX_SLAB_INC88'] == 'FloatType'
#             dict(df.dtypes)['TAXSLAB_INC89'] == 'FloatType'
#             dict(df.dtypes)['TAXSLAB_TAX89'] == 'FloatType'
#     assert(df.schema(TAX_CODE) match {
#         case StructField(_, IntegerType, nullable, _) => true
#         case _ => false
#         })



@given("I verify the not null checks are as expected")
def step_impl():
    raise NotImplementedError(u'STEP: And  I verify the not null checks are as expected')


@given(
    "I verify the the total record count in the parquet files matches the respective source SAS Files are as expected")
def step_impl():
    raise NotImplementedError(
        u'STEP: And  I verify the the total record count in the parquet files matches the respective source SAS Files are as expected')


@given("I verify the the data in the parquet files matches the respective source SAS Files are as expected")
def step_impl():
    raise NotImplementedError(
        u'STEP: And  I verify the the data in the parquet files matches the respective source SAS Files are as expected')


@when("I download DQ file for verification")
def step_impl():
    raise NotImplementedError(u'STEP: When I download DQ file for verification')


@then("the file should be downloaded sucessfully")
def step_impl():
    raise NotImplementedError(u'STEP: Then the file should be downloaded sucessfully')


@given("I verify the technical columns are correct as per the requirements and data is correctly populated")
def step_impl():
    raise NotImplementedError(
        u'STEP: And  I verify the technical columns are correct as per the requirements and data is correctly populated')


@given("I verify that empty files will have an entry in the DQ File")
def step_impl():
    raise NotImplementedError(u'STEP: And  I verify that empty files will have an entry in the DQ File')


@given("I verify that valid schema functionality is working as expected")
def step_impl():
    raise NotImplementedError(u'STEP: And  I verify that valid schema functionality is working as expected')


@given("I verify that the duplicate files will have an entry in the DQ File")
def step_impl():
    raise NotImplementedError(u'STEP: And  I verify that the duplicate files will have an entry in the DQ File')


@when("verify the reference data checks")
def step_impl():
    raise NotImplementedError(u'STEP: When verify the reference data checks')


@then("reference data checks are working as expected")
def step_impl():
    raise NotImplementedError(u'STEP: Then reference data checks are working as expected')


@given("I verify the data type checks of the columns in the are as expected")
def step_impl():
    raise NotImplementedError(u'STEP: And  I verify the data type checks of the columns in the are as expected')


class SparkSession(object):
    builder = None

class df_with_audit_cols(object):
    builder = None

@then("I verify the not null checks are as expected in the parquet file are as expected")
# IF column EXISTS && NOT NULL and is not null in a DataFrame
def step_test_col_exist_and_not_null(df_with_audit_cols):
    if ('TAX_CODE', 'TAX_SLAB_INC88', 'TAXSLAB_INC89', 'TAXSLAB_TAX89', 'TAXSLAB_TAX89', 'operation',
        'changedate', 'changedate_year', 'changedate_month', 'changedate_month') \
            in df_with_audit_cols.columns and df_with_audit_cols['column6'].notnull().any():
            df_with_audit_cols.select('TAX_CODE', 'TAX_SLAB_INC88', 'TAXSLAB_INC89', 'TAXSLAB_TAX89', 'TAXSLAB_TAX89',
                                      'operation',
                                      'changedate', 'changedate_year', 'changedate_month', 'changedate_month')
    else:
         df_with_audit_cols.select('TAX_CODE', 'TAX_SLAB_INC88', 'TAXSLAB_INC89', 'TAXSLAB_TAX89', 'TAXSLAB_TAX89',
                                      'operation', 'changedate', 'changedate_year', 'changedate_month', 'changedate_month')
         raise ValueError('Decrepany in Columnns')


@then("I verify there are no duplicate records in the parquet file")
def step_verify_duplicate_records(df_with_audit_cols):
    if df_with_audit_cols.count() > df_with_audit_cols.dropDuplicates(['TAX_CODE', 'TAX_SLAB_INC88', 'TAXSLAB_INC89', 'TAXSLAB_TAX89', 'TAXSLAB_TAX89', 'operation',
        'changedate', 'changedate_year', 'changedate_month', 'changedate_month']).count():
        raise ValueError('Data has duplicates')



@then("I verify the data type checks of the columns in the parquet file are as expected")
def test_step_impl(df_with_audit_cols):
    spark = SparkSession.builder.appName('MyApp').getOrCreate()
    df_with_audit_cols = spark.read.csv('Path To PARQUET File', inferSchema=True, header=True)
    assert (dict(df_with_audit_cols.dtypes)['TAX_CODE'] == 'string')
    assert (dict(df_with_audit_cols.dtypes)['TAX_SLAB_INC88'] == 'float')
    assert (dict(df_with_audit_cols.dtypes)['TAX_SLAB_TAX88'] == 'float')
    assert (dict(df_with_audit_cols.dtypes)['TAXSLAB_INC89'] == 'float')
    assert (dict(df_with_audit_cols.dtypes)['TAXSLAB_TAX89'] == 'float')
    assert (dict(df_with_audit_cols.dtypes)['TAXSLAB_TAX89'] == 'float')
    assert (dict(df_with_audit_cols.dtypes)['operation'] == 'string')
    assert (dict(df_with_audit_cols.dtypes)['changedate'] == 'string')
    assert (dict(df_with_audit_cols.dtypes)['changedate_year'] == 'string')
    assert (dict(df_with_audit_cols.dtypes)['changedate_month'] == 'string')
    assert (dict(df_with_audit_cols.dtypes)['changedate_day'] == 'string')