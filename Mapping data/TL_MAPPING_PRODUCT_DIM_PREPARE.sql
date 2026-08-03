USE [ClusterDWH]
GO
/****** Object:  StoredProcedure [off].[TL_MAPPING_PRODUCT_DIM_PREPARE]    Script Date: 8/3/2026 8:28:21 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [off].[TL_MAPPING_PRODUCT_DIM_PREPARE] (@inDateKey nvarchar(6), @inSource nvarchar(255))
as 
DECLARE @vSQL varchar(4000);
DECLARE @vMsg varchar(4000);
DECLARE @vSourceSystemKey integer;
DECLARE @vDateKey integer;
DECLARE @cntCheck integer;
DECLARE @CaseSensitive integer;
DECLARE @Source varchar(4000);
BEGIN

SELECT @vDateKey = DATE_KEY FROM [off].TL_DATES WHERE DATE_KEY = cast(@inDateKey as integer) ;
SELECT @vSourceSystemKey = SOURCE_SYSTEM_KEY FROM [off].TL_SOURCE_SYSTEM WHERE SOURCE_SYSTEM_NAME = @inSource;
SELECT @CaseSensitive = PRODUCT_CASE_SENSITIVE FROM [off].TL_SOURCE_SYSTEM WHERE SOURCE_SYSTEM_NAME = @inSource;
select @Source = case when @inSource like 'GGT%' then 'GGT' ELSE @inSource END ;

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

	select @cntCheck = COUNT(*) FROM [off].TL_BRIDGE_MAPPING_PRODUCT WHERE SOURCE_SYSTEM_KEY = @vSourceSystemKey and DATE_KEY = CAST(@inDateKey as integer) ;
	IF @cntCheck > 0 
		BEGIN
	DELETE FROM [off].TL_BRIDGE_MAPPING_PRODUCT WHERE SOURCE_SYSTEM_KEY = @vSourceSystemKey and DATE_KEY = CAST(@inDateKey as integer);
	   END;
			SELECT @vSQL =
		'SELECT distinct '+
		CASE WHEN @CaseSensitive = 0 THEN
			'RTRIM(LTRIM(UPPER(s.'+cm.PRODUCT_BRIDGE_EXTERNAL_COLUMN+')))  as PRODUCT_ID_EXTERNAL '
		ELSE
			'RTRIM(LTRIM(s.'+cm.PRODUCT_BRIDGE_EXTERNAL_COLUMN+'))  as PRODUCT_ID_EXTERNAL '
		END
				+' ,a.CODE AS PRODUCT_ID_INTERNAL 
				,'+@inDateKey+' DATE_KEY
				,src.SOURCE_SYSTEM_KEY SOURCE_SYSTEM_KEY
				,(case when a.CODE is null or s.'+cm.PRODUCT_BRIDGE_EXTERNAL_COLUMN +' is null then 0 ELSE 1 END) AS mapping
				,a.ID
			 FROM [off].['+SOURCE_SYSTEM_TABLE+'] s 
			full outer join (
			select cb.EXTERNAL_PRODUCT_CODE, 
				 a.CODE,
				 a.PRODUCT_NK AS ID,
				 ROW_NUMBER() OVER (PARTITION BY cb.EXTERNAL_PRODUCT_CODE,A.CODE ORDER BY VALID_FROM DESC) rn
			 from [off].[TL_DIM_PRODUCT] a 
			 JOIN  [off].[IN_PRODUCTS_BRIDGE] cb
								on cb.INTERNAL_PRODUCT_CODE = a.CODE 
								--AND '+@inDateKey+' BETWEEN a.VALID_FROM AND a.VALID_TO
								and cb.CUSTOMER_NAME = '''+@inSource+'''
					) a ON ' +
			CASE WHEN @CaseSensitive = 0 THEN
				  'RTRIM(LTRIM(UPPER(s.'+cm.PRODUCT_BRIDGE_EXTERNAL_COLUMN+'))) = UPPER(a.EXTERNAL_PRODUCT_CODE)'
			ELSE  
				  'RTRIM(LTRIM(s.'+cm.PRODUCT_BRIDGE_EXTERNAL_COLUMN+')) = a.EXTERNAL_PRODUCT_CODE'
			END
			+' join [off].TL_SOURCE_SYSTEM src on src.SOURCE_SYSTEM_NAME = '''+@inSource+'''
		WHERE  isnull(a.rn,1) =1  and s.DATE_KEY ='+@inDateKey
			FROM [off].TL_SOURCE_SYSTEM ss
			JOIN [off].IN_BRIDGE_COLUMN_MAPPING cm
				ON  ss.SOURCE_SYSTEM_NAME = cm.SOURCE_SYSTEM_NAME
				AND cm.DATE_KEY =  CAST(@inDateKey as integer)
			WHERE ss.SOURCE_SYSTEM_NAME = @inSource; 





			SET @vSQL = 'INSERT INTO  [off].TL_BRIDGE_MAPPING_PRODUCT  (
			PRODUCT_ID_EXTERNAL
			,PRODUCT_ID_INTERNAL
			,DATE_KEY
			,SOURCE_SYSTEM_KEY
			,IS_MAPPED
			,PRODUCT_KEY)  '+@vSQL;

			EXECUTE (@vSQL);
			print @vSQL;

		UPDATE STATISTICS [off].[TL_BRIDGE_MAPPING_PRODUCT] [TL_BRIDGE_MAPPING_PRODUCT_IDX]; 
/*	RAISERROR (N'Source does not exists', -- Message text.
           10, -- Severity,
           1, -- State,
           N'number', -- First argument.
           5); -- Second argument.*/
END;


