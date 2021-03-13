#Scenarios covered as part of this feature file End to End testing of aws glue job - 'Glue_Job_SAS_To_Parquet':
#@etl-glue-job-testing
#@file-ingestion-testing
#@Data-type-verification
#@Data-completeness-testing
#@Data-quality-testing
#@Operational-data-quality-checks-DQ-files
#@Reference-data-checks

Feature: SAS S3 bucket to Parquet S3 Bucket File Transfer
  As a user,
  I want to tranfer the file in SAS Format from SAS S3 Buket in AWS to Parquet Format and load in to the target Parquet S3 Bucket,
  So that I can access the Parquet file with audit columns

  @etl-glue-job-testing
  Scenario Outline: Run the AWS Glue job that transfers the file from source SAS S3 to target Parquet S3
    Given the SASfile is in Source AWS S3 "<sourceBucket>" with <SASfileName>
    When I run the Gluejob with <jobName> to transfer the file
    And the Gluejob <jobName> is sucessfully run
    Then I should see the <parquetFileName> is in the <targetbucketName>" in parquet Format
    Examples:
      | sourceBucket           | SASfileName            | jobName                          | targetbucketName             | parquetFileName       |
      | delivery-july-2020     | insur.sas7bdat         |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  insur.parquet        |
      | delivery-july-2020     | savings.sas7bdat       |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  savings.parquet      |
      | delivery-july-2020     | creditcards.sas7bdat   |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  creditcards.parquet  |
      | delivery-july-2020     | current.sas7bdat       |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  current.parquet      |
      | delivery-july-2020     | bonds.sas7bdat         |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  bonds.parquet        |
      | delivery-july-2020     | mortgages.sas7bdat     |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  mortgages.parquet    |
      | delivery-july-2020     | paymentinsur.sas7bdat  |   python-job-cli-sas-parquet     | insur-delivery-july-2020     |  paymentinsur.parquet |

  @file-ingestion-testing
  Scenario Outline: Verify the inbound File name, File pattern along with the business date e.g. daily, monthly, weekly runs, addition of audit columns
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When I download the file for verification
    Then the file should be downloaded in parquet Format
    And  the file name pattern along with the business date, daily, weekly run info is as expected
    And  the <auditcolumns> are added are as expected
    And the values in the <auditcolumns> are correct
     Examples:
      | auditcolumns      |
      | operation         |
      | processeddate     |
      | changedate        |
      | changedate_year   |
      | changedate_month  |
      | changedate_day    |

  @Data-type-verification
  Scenario: Verify the data length checks - Length of the strings and number data values in the parquet file
            should match the maximum allowed length for those columns
            Not null checks - Verify that all the required/mandatory data elements in the parquet file
            have data for all the rows e.g. primary keys/rows are unique i.e. no duplicates and other business mandatory fields when compared with the source file with an exception to audit colums
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When I download the file for verification
    Then the file should be downloaded in parquet Format
    And  I verify the not null checks are as expected in the parquet file are as expected
    And  I verify the data type checks of the columns in the parquet file are as expected
    And  I verify there are no duplicate records in the parquet file

  @Data-completeness-testing
  Scenario: Verify that the total record count and data in the parquet files matches the respective source SAS Files
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When I download the file for verification
    Then the file should be downloaded in parquet Format
    And  I verify the the total record count in the parquet files matches the respective source SAS Files are as expected
    And  I verify the the data in the parquet files matches the respective source SAS Files are as expected

  @Data-quality-testing
  Scenario: Verify the data validation rules i.e. cases where fields with values violating the rules e.g. DOB in future date etd
  in the parquet files matches the respective source SAS Files
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When I download the file for verification
    Then the file should be downloaded in parquet Format
    And  I verify the the total record count in the parquet files matches the respective source SAS Files are as expected
    And  I verify the the data in the parquet files matches the respective source SAS Files are as expected

  @Operational-data-quality-checks-DQ-files
  Scenario: Verify the technical columns are correct as per the requirements and data is correctly populated
            Verify that the empty files will have an entry in the DQ File
            Verify that valid schema functionality is working as expected
            Verify that the duplicate files will have an entry in the DQ File
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When I download DQ file for verification
    Then the file should be downloaded sucessfully
    And  I verify the technical columns are correct as per the requirements and data is correctly populated
    And  I verify that empty files will have an entry in the DQ File
    And  I verify that valid schema functionality is working as expected
    And  I verify that the duplicate files will have an entry in the DQ File

  @Reference-data-checks
  Scenario: Verify the technical columns are correct as per the requirements and data is correctly populated
            Verify that the empty files will have an entry in the DQ File
            Verify that valid schema functionality is working as expected
            Verify that the duplicate files will have an entry in the DQ File
    Given the SASfile that is transferred in the target parquet AWS S3 bucket
    When verify the reference data checks
    Then reference data checks are working as expected
