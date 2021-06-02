--Metadata Table
CREATE TABLE [dev_test_db].[adfmetadata_tbl](
  ID bigint NOT NULL, 
  EntityName nvarchar(4000) COLLATE Latin1_General_100_BIN2 NOT NULL, 
  ADLSGen2_Path nvarchar(1000) COLLATE Latin1_General_100_BIN2 NULL, 
  ADBSchemaName nvarchar(1000) COLLATE Latin1_General_100_BIN2 NULL, 
  ADBDestinationTableName nvarchar(1000) COLLATE Latin1_General_100_BIN2 NULL, 
  EnabledForIncrementalLoad nvarchar(1) COLLATE Latin1_General_100_BIN2 NULL, 
  SubStoredProcedureName nvarchar(1000) COLLATE Latin1_General_100_BIN2 NULL, 
  JSON_Structure nvarchar(MAX) COLLATE Latin1_General_100_BIN2 NULL
)
--Stored Procedure to validate files in ADLS2 with Metadata Table.  
CREATE procedure [dev_test_db].[usp_StartTrackingstatus] (
  @Blobpath varchar(max), 
  @FileName varchar(max), 
  @PipelineRunID varchar(100), 
  @PipelineName varchar(500), 
  @DatafactoryName varchar(500), 
  @TriggerName varchar(500), 
  @Triggertype varchar(100), 
  @Triggeredtime datetime2
) AS BEGIN DECLARE @FullFilePath varchar(max);
DECLARE @DT DateTime;
DECLARE @DTEnd DateTime;
DECLARE @EntityName Varchar(1000);
DECLARE @ProductName Varchar(1000);
DECLARE @ContainerName Varchar(1000);
Declare @Latestblobpath nvarchar(max);
DECLARE @ADBSchemaName Varchar(1000);
DECLARE @ADWSchemaName Varchar(1000) DECLARE @ADBDestinationTableName Varchar(1000);
DECLARE @EnabledForIncrementalLoad Varchar(10);
DECLARE @SubStoredProcedureName Varchar(1000);
Declare @ADLSGen2_path nvarchar(max);
Declare @JsonStructure nvarchar(max);
DECLARE @ID BigInt;
DECLARE @FilesToProcessID Bigint;
DECLARE @ErMsg Varchar(4000);
DECLARE @msg varchar(8000);
Declare @index1 int;
Declare @index2 int;
Declare @index3 int;
Declare @index4 int;
Declare @index5 int;
Declare @index6 int;
DECLARE @NewID UniqueIdentifier;
DECLARE @EntityInError char(1);
DECLARE @EntityInErrorMessage varchar(500);
DECLARE @functionality int;
DECLARE @batchid varchar(36);
BEGIN TRY 
SET 
  @FileName = LTRIM(
    RTRIM(@FileName)
  );
SET 
  @Blobpath = LTRIM(
    RTRIM(@Blobpath)
  );
SET 
  @FullFilePath = @Blobpath + '' + @FileName;
SET 
  @DT = GETUTCDATE();
SET 
  @index1 = CHARINDEX('/', @Blobpath);
IF (@index1 = 0) BEGIN 
SET 
  @ErMsg = 'Could not find Seperator';
Raiserror(@ErMsg, 16, 1);
END 
SET 
  @ContainerName = SUBSTRING(@Blobpath, 1, @index1 -1) PRINT 'Container Name ' + @ContainerName 
SET 
  @index2 = CHARINDEX('/', @Blobpath, @index1 + 1);
SET 
  @index3 = CHARINDEX('/', @Blobpath, @index2 + 1);
SET 
  @index6 = CHARINDEX('/', @Blobpath, @index3 + 1);
SET 
  @index4 = CHARINDEX('~', @Blobpath, @index3 + 1);
SET 
  @index5 = CHARINDEX('~', @Blobpath, @index4 + 1);
IF (
  @index2 = 0 
  or @index3 = 0 
  or @index4 = 0 
  or @index5 = 0
) BEGIN 
SET 
  @ErMsg = 'Could not find Seperator';
Raiserror(@ErMsg, 16, 1);
END 
SET 
  @ProductName = SUBSTRING(
    @Blobpath, 
    @index1 + 1, 
    @index2 -(@index1 + 1)
  );
PRINT 'Product Name ' + @ProductName 
SET 
  @EntityName = SUBSTRING(
    @Blobpath, 
    @index3 + 1, 
    @index6 -(@index3 + 1)
  );
PRINT 'Search Entity ' + @EntityName 
SET 
  @batchid = SUBSTRING(
    @Blobpath, 
    @index4 + 1, 
    @index5 -(@index4 + 1)
  );
PRINT 'Batch ID ' + @batchid 
SELECT 
  @ID = ID, 
  @EntityName = EntityName, 
  @ADBSchemaName = ADBSchemaName, 
  @ADBDestinationTableName = ADBDestinationTableName, 
  @ADLSGen2_path = [ADLSGen2_Path], 
  @EnabledForIncrementalLoad = EnabledForIncrementalLoad, 
  @SubStoredProcedureName = [SubStoredProcedureName], 
  @JsonStructure = [JSON_Structure] 
FROM 
  [dev_test_db].[adfmetadata_tbl] 
Where 
  UPPER(
    LTRIM(
      RTRIM([EntityName])
    )
  ) = UPPER(
    LTRIM(
      RTRIM(@EntityName)
    )
  ) IF(@ID IS NULL) BEGIN 
SET 
  @ErMsg = 'This File is not Setup for Ingestion ' + @FullFilePath;
Raiserror(@ErMsg, 16, 1);
END ELSE BEGIN 
SET 
  @NewID = NewID();
EXEC [dev_test_db].[insert_FilesToProcess] @ADWID = @NewID, 
@batchid = @batchid, 
@Blobpath = @Blobpath, 
@FileName = @FileName, 
@FilePath = @FullFilePath, 
@EntityID = @ID, 
@EntityName = @EntityName, 
@ProductName = @ProductName, 
@DatafactoryName = @DatafactoryName, 
@PipelineRunID = @PipelineRunID, 
@PipelineName = @PipelineName, 
@TriggerName = @TriggerName, 
@Triggertype = @Triggertype 
Select 
  @FilesToProcessID = ID 
From 
  [dev_test_db].Batch_Execution_Detail_Statistic 
Where 
  Consumption_Data_Warehouse_Id = @NewID;
IF @@ROWCOUNT = 0 BEGIN 
SET 
  @ErMsg = 'Error Retrieving Record from Customer_Service_Analytics.Batch_Execution_Detail_Statistic Table for Consumption_Data_Warehouse_Id  ' + CAST(
    @NewID AS Varchar(36)
  );
PRINT @ErMsg;
Raiserror(@ErMsg, 16, 1);
END ELSE BEGIN 
Select 
  @ID as EntityID, 
  @FilesToProcessID as FileToProcessID, 
  @batchid as batchid, 
  @ProductName as ProductName, 
  @EntityName as EntityName, 
  @Latestblobpath as Latestblobpath, 
  @ADBSchemaName as ADBSchemaName, 
  @ADBDestinationTableName as ADBDestinationTableName, 
  @EnabledForIncrementalLoad as EnabledForIncrementalLoad, 
  @Duration_in_Yrs as Duration_in_Yrs, 
  @PrimaryKey as PrimaryKey, 
  @SubStoredProcedureName as SubStoredProcedureName, 
  @JsonStructure as JsonStructure;
END END END TRY END
