USE [ClusterDWH]
GO
/****** Object:  StoredProcedure [off].[TL_MAPPING_ACCOUNT_DIM_PREPARE]    Script Date: 8/3/2026 8:28:16 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [off].[TL_MAPPING_ACCOUNT_DIM_PREPARE] (@inDateKey nvarchar(6), @inSource nvarchar(255))
as 
DECLARE @vSQL varchar(4000);
DECLARE @vMsg varchar(4000);
DECLARE @vSourceSystemKey integer;
DECLARE @vDateKey integer;
DECLARE @cntCheck integer;
DECLARE @CaseSensitive integer;
BEGIN

SELECT @vDateKey = DATE_KEY FROM [off].TL_DATES WHERE DATE_KEY = cast(@inDateKey as integer) ;
SELECT @vSourceSystemKey = SOURCE_SYSTEM_KEY FROM [off].TL_SOURCE_SYSTEM WHERE SOURCE_SYSTEM_NAME = @inSource;
SELECT @CaseSensitive = ACCOUNT_CASE_SENSITIVE FROM [off].TL_SOURCE_SYSTEM WHERE SOURCE_SYSTEM_NAME = @inSource;

		IF @vDateKey is null 
			BEGIN 
				SET @vMsg = 'inDateKey - DATEKEY = '''+@inDateKey+''' does not exists';
					RAISERROR (15600, -1, -1,@vMsg );
			END;
		IF @vSourceSystemKey is null 
			BEGIN 
				SET @vMsg = 'inSource - SOURCE =  '''+@inSource+''' does not exists';
					RAISERROR (15600, -1, -1,@vMsg );
			END;

	select @cntCheck = COUNT(*) FROM [off].TL_BRIDGE_MAPPING_ACCOUNT WHERE SOURCE_SYSTEM_KEY = @vSourceSystemKey and DATE_KEY = CAST(@inDateKey as integer) ;
	IF @cntCheck > 0 
		BEGIN
	DELETE FROM [off].TL_BRIDGE_MAPPING_ACCOUNT WHERE SOURCE_SYSTEM_KEY = @vSourceSystemKey and DATE_KEY = CAST(@inDateKey as integer);
	   END;
			SELECT @vSQL =
			'SELECT distinct '+ 
				CASE WHEN  @CaseSensitive = 0 THEN
					CASE WHEN @inSource = 'COOP' THEN 
						'CAST(RTRIM(LTRIM(UPPER(SUBSTRING(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+',1,CHARINDEX('' '',s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+'))))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) as ACCOUNT_ID_EXTERNAL '
					else
						'CAST(RTRIM(LTRIM(UPPER(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+'))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) as ACCOUNT_ID_EXTERNAL '
					end
				ELSE 
					CASE WHEN @inSource = 'COOP' THEN 
						'CAST(RTRIM(LTRIM((SUBSTRING(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+',1,CHARINDEX('' '',s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+'))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) as ACCOUNT_ID_EXTERNAL '
					else 
						'CAST(RTRIM(LTRIM(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+')) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) as ACCOUNT_ID_EXTERNAL '
					end
				END
				+' ,a.ACCOUNT_CODE AS ACCOUNT_ID_INTERNAL 
				,'+@inDateKey+' DATE_KEY
				,src.SOURCE_SYSTEM_KEY SOURCE_SYSTEM_KEY
				,(case when a.ACCOUNT_CODE is null or s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN +' is null then 0 ELSE 1 END) AS mapping
				,a.ID
			 FROM [off].['+ss.SOURCE_SYSTEM_TABLE+'] s 
			full outer join (
			select cb.EXTERNAL_ACCOUNT_CODE, 
				 a.ACCOUNT_CODE,
				 a.ACCOUNT_NK AS ID,
				 ROW_NUMBER() OVER (PARTITION BY cb.EXTERNAL_ACCOUNT_CODE, A.ACCOUNT_CODE ORDER BY VALID_FROM DESC) rn
			 from [off].[TL_DIM_ACCOUNT] a 
			 JOIN  [off].[IN_ACCOUNTS_BRIDGE] cb
								on cb.INTERNAL_ACCOUNT_CODE = a.ACCOUNT_CODE 
								--and '+@inDateKey+' between a.VALID_FROM and a.VALID_TO
								and cb.CUSTOMER_NAME = '''+@inSource+'''
				 ) a ON '+

				CASE WHEN  @CaseSensitive = 0 THEN
					CASE WHEN @inSource = 'COOP' THEN 
						'CAST(RTRIM(LTRIM(UPPER(SUBSTRING(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+',1,CHARINDEX('' '',s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+'))))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) = CAST(UPPER(a.EXTERNAL_ACCOUNT_CODE) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) '
						when @inSource = 'GGT WHS' THEN 
						's.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+' = a.EXTERNAL_ACCOUNT_CODE '
					else
						'CAST(RTRIM(LTRIM(UPPER(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+'))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) = CAST(UPPER(a.EXTERNAL_ACCOUNT_CODE) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) '
					end
				ELSE 
					CASE WHEN @inSource = 'COOP' THEN 
						'CAST(RTRIM(LTRIM((SUBSTRING(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+',1,CHARINDEX('' '',s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+')))) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) = CAST(a.EXTERNAL_ACCOUNT_CODE COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) '
						when @inSource = 'GGT WHS' THEN 
						's.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+' = a.EXTERNAL_ACCOUNT_CODE '
					else 
						'CAST(RTRIM(LTRIM(s.'+cm.ACCOUNT_BRIDGE_EXTERNAL_COLUMN+')) COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) = CAST(a.EXTERNAL_ACCOUNT_CODE COLLATE SQL_Slovak_CP1250_CS_AS  AS NVARCHAR ) '
					end
				END

			+ ' join [off].TL_SOURCE_SYSTEM src on src.SOURCE_SYSTEM_NAME = '''+@inSource+'''
			where isnull(a.rn,1) = 1 and s.DATE_KEY ='+@inDateKey
			--SELECT * FROM  [off].IN_BRIDGE_COLUMN_MAPPING cm
			FROM [off].TL_SOURCE_SYSTEM ss
		JOIN [off].IN_BRIDGE_COLUMN_MAPPING cm
			ON  ss.SOURCE_SYSTEM_NAME = cm.SOURCE_SYSTEM_NAME
			AND cm.DATE_KEY =  CAST(@inDateKey as integer)
			WHERE ss.SOURCE_SYSTEM_NAME = @inSource; 

			SET @vSQL = 'INSERT INTO  [off].TL_BRIDGE_MAPPING_ACCOUNT (
			ACCOUNT_ID_EXTERNAL
			,ACCOUNT_ID_INTERNAL
			,DATE_KEY
			,SOURCE_SYSTEM_KEY
			,IS_MAPPED
			,ACCOUNT_KEY) '+@vSQL;

			print @vSQL;
			EXECUTE (@vSQL);
			
			UPDATE STATISTICS [off].TL_BRIDGE_MAPPING_ACCOUNT [TL_BRIDGE_MAPPING_ACCOUNT_IDX];  

/*	RAISERROR (N'Source does not exists', -- Message text.
           10, -- Severity,
           1, -- State,
           N'number', -- First argument.
           5); -- Second argument.*/
END;
