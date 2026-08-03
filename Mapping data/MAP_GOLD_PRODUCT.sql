USE [ClusterDWH]
GO
/****** Object:  StoredProcedure [off].[MAP_GOLD_PRODUCT]    Script Date: 8/3/2026 8:31:39 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [off].[MAP_GOLD_PRODUCT](@inPhase varchar(50)
											 ,@inJobID	varchar(200)
											 ,@inSource varchar(200)
											 )
as 



BEGIN
 

IF @inPhase = 'FUZZY GROUPING'
BEGIN
with base_src  as (
	select (CASE WHEN @inSource = 'ALL' THEN 0
				 WHEN [off].GetSourceSystemKey(@inSource) != -1 then [off].GetSourceSystemKey(@inSource)
		else -999 end ) res
)
,src as (
	SELECT
		SOURCE_SYSTEM_KEY
	FROM [off].TL_SOURCE_SYSTEM s
	join base_src on base_src.res = 0 or base_src.res=s.SOURCE_SYSTEM_KEY
	)
,m1 as (
			select DISTINCT 
				m.name ,
				m.norm_name, 
				m.MANUFACTURER,
				M.BRAND,
				M.CATEGORY,
				m.SUBCATEGORY,
				m.CONVERSION_PACK,
				m.product_key,
				m.source_system_key,
				case when m.name like '% 100%' then 1 else 0 end as name_comp,
				FIRST_VALUE(replace(b.BRAND,'LM','L&M')) OVER (PARTITION BY M.NAME ORDER BY B.BRAND) AS B_BRAND,
				FIRST_VALUE(c.COLOR) OVER (PARTITION BY M.NAME ORDER BY c.COLOR) AS COLOR
			from [off].[IN_FOREIGN_PRODUCT_MAPPING] /*IN_FOREIGN_PRODUCT_MATCH_MANUAL_REVIEW*/ m
			left join [off].[TL_DIM_FOREIGN_BRAND] b on upper(replace(UPPER(m.name),'L&M','LM')) like '%'+b.brand+'%'
			left join [off].TL_DIM_COLOR c on upper(m.name) like '%'+c.COLOR+'%'
			where m.source_system_key in (select source_system_key from src)
			)
			,m2 as (
				select DISTINCT
					F_PRODUCT_KEY,
					NAME,
					MANUFACTURER,
					p.BRAND,
					CATEGORY,
					CONVERSION_PACK,
					SUBCATEGORY,
					case when name like '% 100%' then 1 else 0 end as name_comp,
					FIRST_VALUE(c.COLOR) OVER (PARTITION BY p.NAME ORDER BY c.COLOR) AS COLOR
				from [off].TL_DIM_FOREIGN_PRODUCT p
				left join [off].TL_DIM_COLOR c on upper(p.name) like '%'+c.COLOR+'%'
			)
			,base as (
			select
			   m1.[NAME]
			  ,m1.[NORM_NAME]
			  ,m1.[MANUFACTURER]
			  ,m1.[BRAND]
			  ,m1.[CATEGORY]
			  ,m1.[PRODUCT_KEY]
			  ,m1.[SOURCE_SYSTEM_KEY]
			  ,m2.NAME AS PROPOSAL_NAME
			  ,m2.F_PRODUCT_KEY AS PROPOSAL_F_PRODUCT_KEY
			  ,[off].FuzzyMatching(m1.NORM_NAME, upper(m2.NAME))  AS MATCH_SCORE
			  ,0 as [CONFIDENCE_SCORE]
			  --,NULL AS RESULT
			  --,NULL AS PROCESS_STATUS
			  --,123 AS JOB_ID
			  --,GETDATe() AS CREATE_DATE
			  ,m1.CONVERSION_PACK
			  ,m1.SUBCATEGORY
			  ,m2.brand as m2_brand
			  ,m1.B_BRAND as m1_brand
			  ,m1.name_comp as m1_name_comp
			  ,m2.name_comp as m2_name_comp
			  ,m1.color as m1_color
			  ,m2.color as m2_color
			from m1 
			JOIN m2 on 1=1
			)
			,base_ext as (
			 SELECT
			  NAME 
			  ,NORM_NAME
			  ,MANUFACTURER
			  ,BRAND
			  ,CATEGORY
			  ,PRODUCT_KEY
			  ,SOURCE_SYSTEM_KEY
			  ,PROPOSAL_NAME
			  ,PROPOSAL_F_PRODUCT_KEY
			  ,MATCH_SCORE
			  ,CASE WHEN  isnull(m1_brand,'yyy') = isnull(m2_brand,'xxx')  THEN 1.2 ELSE MATCH_SCORE END AS MATCH_SCORE_BRAND
			  ,CASE WHEN  m1_name_comp = m2_name_comp  AND MATCH_SCORE >= 0.9 THEN 1 
					WHEN  m1_name_comp = m2_name_comp  AND MATCH_SCORE < 0.9 THEN 0.95
					 ELSE MATCH_SCORE  END AS MATCH_SCORE_NAME_COMP
			  ,CASE WHEN  m1_color = m2_color  AND MATCH_SCORE >= 0.9 THEN 1 
					WHEN  m1_color = m2_color  AND MATCH_SCORE < 0.9 THEN 0.95
					 ELSE MATCH_SCORE  END AS MATCH_SCORE_COLOR
			  ,match_Score as CONFIDENCE_SCORE
			  ,CONVERSION_PACK
			  ,SUBCATEGORY
			FROM BASE
			)
			,match_score_calc as (
			SELECT
			 NAME 
			  ,NORM_NAME
			  ,MANUFACTURER
			  ,BRAND
			  ,CATEGORY
			  ,PRODUCT_KEY
			  ,SOURCE_SYSTEM_KEY
			  ,PROPOSAL_NAME
			  ,PROPOSAL_F_PRODUCT_KEY
			  ,(MATCH_SCORE+MATCH_SCORE_BRAND+MATCH_SCORE_NAME_COMP+MATCH_SCORE_COLOR)/4 AS MATCH_SCORE 
			  ,MAX((MATCH_SCORE+MATCH_SCORE_BRAND+MATCH_SCORE_NAME_COMP+MATCH_SCORE_COLOR)/4) OVER (PARTITION BY NAME,MANUFACTURER,CATEGORY,CONVERSION_PACK,SOURCE_SYSTEM_KEY) AS MAX_MATCH_SCORE
			  ,match_Score as CONFIDENCE_SCORE
			  ,CONVERSION_PACK
			  ,SUBCATEGORY
			FROM base_ext
			)
			,fin as (
				SELECT  
				  ROW_NUMBER() OVER (PARTITION BY  NAME,MANUFACTURER,CATEGORY,CONVERSION_PACK,SOURCE_SYSTEM_KEY order by  MATCH_SCORE DESC) rn
				  ,DENSE_RANK() OVER (PARTITION BY 1 ORDER BY NAME,MANUFACTURER,CATEGORY,CONVERSION_PACK,SOURCE_SYSTEM_KEY) as grp_id
				  ,NAME 
				  ,NORM_NAME
				  ,MANUFACTURER
				  ,BRAND
				  ,CATEGORY
				  ,PRODUCT_KEY
				  ,SOURCE_SYSTEM_KEY
				  ,PROPOSAL_NAME
				  ,PROPOSAL_F_PRODUCT_KEY
				  , MATCH_SCORE
				  ,CONFIDENCE_SCORE
				  ,CONVERSION_PACK
				  ,SUBCATEGORY
				FROM match_score_calc
				WHERE MAX_MATCH_sCORE >= 0.85 AND MATCH_sCORE >= 0.85
			UNION ALL
				SELECT  DISTINCT
				   1 rn
				  ,DENSE_RANK() OVER (PARTITION BY 1 ORDER BY NAME,MANUFACTURER,CATEGORY,CONVERSION_PACK,SOURCE_SYSTEM_KEY) as grp_id
				  ,NAME 
				  ,NORM_NAME
				  ,MANUFACTURER
				  ,BRAND
				  ,CATEGORY
				  ,PRODUCT_KEY
				  ,SOURCE_SYSTEM_KEY
				  ,NULL PROPOSAL_NAME
				  ,NULL PROPOSAL_F_PRODUCT_KEY
				  ,0 MATCH_SCORE
				  ,0 CONFIDENCE_SCORE
				  ,CONVERSION_PACK
				  ,SUBCATEGORY
				FROM match_score_calc
			WHERE MAX_MATCH_sCORE < 0.85
			)
INSERT INTO [off].IN_FOREIGN_PRODUCT_MATCH (
	   [NAME]
      ,[NORM_NAME]
      ,[MANUFACTURER]
      ,[BRAND]
      ,[CATEGORY]
      ,[PRODUCT_KEY]
      ,[SOURCE_SYSTEM_KEY]
      ,[PROPOSAL_NAME]
      ,[PROPOSAL_F_PRODUCT_KEY]
      ,[MATCH_SCORE]
      ,[CONFIDENCE_SCORE]
      ,[RESULT]
      ,[PROCESS_STATUS]
      ,[JOB_ID]
      ,[CREATE_DATE]
      ,[CONVERSION_PACK]
      ,[SUBCATEGORY]
	  )
			SELECT
			  NAME
			  ,NORM_NAME
			  ,MANUFACTURER
			  ,BRAND
			  ,CATEGORY
			  ,PRODUCT_KEY
			  ,SOURCE_SYSTEM_KEY
			  ,PROPOSAL_NAME
			  ,PROPOSAL_F_PRODUCT_KEY
			  , MATCH_SCORE
			  ,CONFIDENCE_SCORE
			  ,NULL AS RESULT
			  ,NULL AS PROCESS_STATUS
			  ,@inJobID AS JOB_ID
			  ,GETDATE() AS CREATE_DATE
			  ,CONVERSION_PACK
			  ,SUBCATEGORY
			FROM fin
			WHERE  rn < 6 
			order by grp_id,match_Score desc;

END;

ELSE IF @inPhase = 'HISTORIZATION MATCH'
	BEGIN
	INSERT INTO [off].[IN_FOREIGN_PRODUCT_MATCH_HIST]
			   ([NAME]
			   ,[NORM_NAME]
			   ,[MANUFACTURER]
			   ,[BRAND]
			   ,[CATEGORY]
			   ,[SUBCATEGORY]
			   ,[CONVERSION_PACK]
			   ,[PRODUCT_KEY]
			   ,[SOURCE_SYSTEM_KEY]
			   ,[PROPOSAL_NAME]
			   ,[PROPOSAL_F_PRODUCT_KEY]
			   ,[MATCH_SCORE]
			   ,[CONFIDENCE_SCORE]
			   ,[RESULT]
			   ,[PROCESS_STATUS]
			   ,[JOB_ID]
			   ,[CREATE_DATE])
		 SELECT [NAME]
			   ,[NORM_NAME]
			   ,[MANUFACTURER]
			   ,[BRAND]
			   ,[CATEGORY]
			   ,[SUBCATEGORY]
			   ,[CONVERSION_PACK]
			   ,[PRODUCT_KEY]
			   ,[SOURCE_SYSTEM_KEY]
			   ,[PROPOSAL_NAME]
			   ,[PROPOSAL_F_PRODUCT_KEY]
			   ,[MATCH_SCORE]
			   ,[CONFIDENCE_SCORE]
			   ,[RESULT]
			   ,[PROCESS_STATUS]
			   ,[JOB_ID]
			   ,GETDATE()
		FROM [off].[IN_FOREIGN_PRODUCT_MATCH];

		--CLEANUP table
		TRUNCATE TABLE [off].[IN_FOREIGN_PRODUCT_MATCH];
	END;

ELSE IF @inPhase = 'AUTO RESULT FUZZY'
	BEGIN 

-- Auto Result via fuzzy matching
		MERGE INTO [off].IN_FOREIGN_PRODUCT_MATCH trg 
		using (
		SELECT physloc,RESULT,PROPOSAL_F_PRODUCT_KEY,MATCH_sCORE,
		CASE WHEN first_value(RESULT) over (partition by grp ORDER BY match_score DESC) ='GOLD' 
			THEN FIRST_VALUE(PROPOSAL_F_PRODUCT_KEY) OVER (PARTITION BY grp order by match_score DESC) 
		ELSE PROPOSAL_F_PRODUCT_KEY END AS NEW_PROPOSAL_F_PRODUCT_KEY
		  FROM (
			SELECT DISTINCT	M.%%physloc%% AS physloc,
				CASE WHEN (MATCH_SCORE >= 1 
						 AND LEAD(MATCH_SCORE,1,0) OVER (PARTITION BY NAME,MANUFACTURER,M.BRAND,CATEGORY,SUBCATEGORY,SOURCE_SYSTEM_KEY ORDER BY MATCH_SCORE DESC) < MATCH_SCORE 
						 AND LAG(MATCH_SCORE,1,0) OVER (PARTITION BY NAME,MANUFACTURER,M.BRAND,CATEGORY,SUBCATEGORY,SOURCE_SYSTEM_KEY ORDER BY MATCH_SCORE DESC) = 0
						 AND (case when name like '%100%' then 1 else 0 end) = (case when PROPOSAL_NAME like '%100%' then 1 else 0 end)
						 AND FIRST_VALUE(b.BRAND) OVER (PARTITION BY M.NAME ORDER BY B.BRAND) = FIRST_VALUE(b1.BRAND) OVER (PARTITION BY M.NAME,m.PROPOSAL_NAME ORDER BY B1.BRAND) 
						 AND FIRST_VALUE(c.COLOR) OVER (PARTITION BY M.NAME ORDER BY c.COLOR) = FIRST_VALUE(c1.COLOR) OVER (PARTITION BY M.NAME,m.PROPOSAL_NAME ORDER BY c1.COLOR) 
					 ) or upper(m.NAME) = upper(m.PROPOSAL_NAME)
					 THEN 'GOLD'
					 WHEN MATCH_SCORE = 0 THEN NULL 
				 ELSE 'PROPOSAL'
				 END AS RESULT
				 , isnull(NAME,'x')+isnull(MANUFACTURER,'x')+isnull(M.BRAND,'x')+isnull(CATEGORY,'x')+isnull(SUBCATEGORY,'x')+cast(SOURCE_SYSTEM_KEY as nvarchar)  grp
				 ,MATCH_SCORE
				 , PROPOSAL_F_PRODUCT_KEY
			 FROM [off].IN_FOREIGN_PRODUCT_MATCH  m
			 --FIRST_VALUE(b.BRAND) OVER (PARTITION BY M.NAME ORDER BY B.BRAND) AS COLOR
			left join [off].[TL_DIM_FOREIGN_BRAND] b on upper(replace(UPPER(m.NAME),'L&M','LM')) like '%'+b.brand+'%'
			left join [off].[TL_DIM_FOREIGN_BRAND] b1 on upper(replace(UPPER(m.PROPOSAL_NAME),'L&M','LM'))  like '%'+b1.brand+'%'
			left join [off].TL_DIM_COLOR c on upper(m.NAME) like '%'+c.COLOR+'%'
			left join [off].TL_DIM_COLOR c1 on upper(m.PROPOSAL_NAME) like '%'+c1.COLOR+'%'
			 WHERE process_status is null
			 AND JOB_ID = @inJobID
			) x
		 ) src
		 on (trg.%%physloc%% = src.physloc)
		 when matched then update set
			 trg.RESULT = src.RESULT,
			-- trg.PROPOSAL_F_PRODUCT_KEY = src.NEW_PROPOSAL_F_PRODUCT_KEY,
			 trg.PROCESS_STATUS = 'Automatic Identification FUZZY';
	END;

ELSE IF @inPhase = 'AUTO RESULT EXISTING'
	BEGIN 
-- Auto result via existing mapping
		WITH m1 as (
			select
				m.%%physloc%% AS physloc,
				m.name m1_name,
				m.norm_name m1_norm_name, 
				b.brand as m1_brand,
				case when m.name like '% 100%' then 1 else 0 end as m1_name_comp
			from [off].IN_FOREIGN_PRODUCT_MATCH /*IN_FOREIGN_PRODUCT_MATCH_MANUAL_REVIEW*/ m
			left join [off].[TL_DIM_FOREIGN_BRAND] b on replace(m.name,'L&M','LM') like '%'+b.brand+'%'
			where m.JOB_ID = @inJobID
			)
			,m2 as (
			select
				m.name m2_name,
				b.brand as m2_brand,
				m.GOLD_PRODUCT_NAME,
				m.GOLD_F_PRODUCT_KEY,
				case when m.name like '% 100%' then 1 else 0 end as m2_name_comp
			from [off].TL_DIM_SRC_GOLD_PRODUCT_MAP m
			left join [off].[TL_DIM_FOREIGN_BRAND] b on replace(m.name,'L&M','LM') like '%'+b.brand+'%'
			)
			,base as (
			select
				[off].FuzzyMatching(m1_name, m2_NAME) fuzzy_match,
				m2.m2_name,
				m2.m2_brand,
				m1.physloc,
				m1.m1_name,
				m1.m1_brand,
				m1.m1_norm_name,
				m1.m1_name_comp,
				m2.m2_name_comp,
				m2.GOLD_PRODUCT_NAME,
				m2.GOLD_F_PRODUCT_KEY
			from m1
			join m2 on 1=1 and m1.m1_brand = m2.m2_brand and m1.m1_name_comp = m2.m2_name_comp
			)
			,fin as (
			select
				row_number() over (partition by m1_name order by fuzzy_match desc) rnk,
				physloc,
				m1_name,
				m1_brand,
				m2_name,
				m2_brand,
				fuzzy_match,
			--	[off].FuzzyMatching(m1_norm_name, GOLD_PRODUCT_NAME) fuzzy_match_norm,
				GOLD_PRODUCT_NAME,
				GOLD_F_PRODUCT_KEY,
				m1_norm_name,
				'GOLD' AS RESULT
			 from base 
			where fuzzy_match > 0.9 )
		MERGE INTO [off].IN_FOREIGN_PRODUCT_MATCH trg 
		using fin as src
		 on (trg.%%physloc%% = src.physloc and src.rnk =1)
		 when matched then update set
			 trg.RESULT = 'GOLD',
			 trg.PROPOSAL_F_PRODUCT_KEY = src.GOLD_F_PRODUCT_KEY,
			 trg.PROCESS_STATUS = 'Automatic Identification EXISTING';

	END;

ELSE IF @inPhase = 'AUTO RESULT INSERT'
	BEGIN 
	update [off].IN_FOREIGN_PRODUCT_MATCH set [PROPOSAL_NAME] = 'N/A',PROPOSAL_F_PRODUCT_KEY = -1,RESULT = 'NonIdentified' WHERE [PROPOSAL_NAME] IS NULL;

--insert to [off].TL_DIM_SRC_GOLD_PRODUCT_MAP
		with base as ( 
		select DISTINCT
			pm.NAME,
			p.MANUFACTURER,
			p.BRAND,
			p.CATEGORY,
			p.SUBCATEGORY,
			p.CONVERSION_PACK,
			pm.SOURCE_SYSTEM_KEY,
			p.F_PRODUCT_KEY AS GOLD_F_PRODUCT_KEY,
			p.NAME AS GOLD_PRODUCT_NAME,
			pm.PROCESS_STATUS
		FROM [off].[IN_FOREIGN_PRODUCT_MATCH] pm
		JOIN [off].TL_DIM_FOREIGN_PRODUCT p
			on pm.PROPOSAL_F_PRODUCT_KEY = p.F_PRODUCT_KEY
		WHERE 1=1  and (pm.RESULT = 'GOLD')
		AND pm.JOB_ID = @inJobID
		and not exists ( select 1 from [off].TL_DIM_SRC_GOLD_PRODUCT_MAP map where map.NAME = pm.NAME )
		)
		,prep as ( 
		select ROW_NUMBER() over (partition by name,SOURCE_SYSTEM_KEY order by case when process_status = 'Automatic Identification EXISTING' then 1 
																					   when process_status = 'Automatic Identification FUZZY' then 2
																			   else 3 end ) as rn,
			NAME,
			MANUFACTURER,
			BRAND,
			CATEGORY,
			SUBCATEGORY,
			CONVERSION_PACK,
			SOURCE_SYSTEM_KEY,
			GOLD_F_PRODUCT_KEY,
			GOLD_PRODUCT_NAME,
			PROCESS_STATUS
		from base 
		)
	INSERT INTO [off].TL_DIM_SRC_GOLD_PRODUCT_MAP (
			NAME,
			MANUFACTURER,
			BRAND,
			CATEGORY,
			SUBCATEGORY,
			CONVERSION_PACK,
			SOURCE_SYSTEM_KEY,
			GOLD_F_PRODUCT_KEY,
			GOLD_PRODUCT_NAME,
			PROCESS_STATUS,
			CREATED_DATETIME,
			CREATED_BY,
			JOB_RUN_ID
		)
		select NAME,
			MANUFACTURER,
			BRAND,
			CATEGORY,
			SUBCATEGORY,
			CONVERSION_PACK,
			SOURCE_SYSTEM_KEY,
			GOLD_F_PRODUCT_KEY,
			GOLD_PRODUCT_NAME,
			PROCESS_STATUS,
			GETDATE() AS CREATED_DATETIME,
			CURRENT_USER AS CREATED_BY,
			@inJobID AS JOB_RUN_ID
		from prep where rn = 1;

	END;

ELSE IF @inPhase = 'GOLD MERGE'
	BEGIN 
		MERGE INTO [off].[TL_DIM_FOREIGN_PRODUCT] trg
			using (
				select 
					p.F_PRODUCT_KEY as DIM_F_PRODUCT_KEY,
					gi.*,
					(CASE WHEN gi.[CONVERSION_PACK] = '' THEN 1 ELSE ISNULL(CAST(gi.[CONVERSION_PACK] AS NUMERIC),1) END)  AS NEW_CONVERSION_PACK
				from [off].[IN_FOREIGN_PRODUCT_GOLD_IMPORT] gi
				left join [off].[TL_DIM_FOREIGN_PRODUCT] p on p.NAME = gi.NAME
				where JOB_ID = @inJobID
				and gi.NAME IS NOT NULL
				) src
		on src.NAME = trg.NAME
		WHEN MATCHED /* and src.DIM_F_PRODUCT_KEY = trg.F_PRODUCT_KEY*/ AND 
			(	trg.[MANUFACTURER] != src.[MANUFACTURER] or
				trg.[BRAND] != src.[BRAND] or
				trg.[CATEGORY] != src.[CATEGORY] or
				trg.[SUBCATEGORY] != src.[SUBCATEGORY] or
				trg.[CONVERSION_PACK] = src.[NEW_CONVERSION_PACK]
				)
		THEN UPDATE SET 
			 [MANUFACTURER] = src.[MANUFACTURER] 
			,[BRAND] = src.[BRAND]
			,[CATEGORY] = src.[CATEGORY]
			,[SUBCATEGORY] = src.[SUBCATEGORY]
			,[CONVERSION_PACK] = src.[NEW_CONVERSION_PACK]
			,[UPDATED_DATE] = getdate()
			,[UPDATED_BY] = CURRENT_USER
		WHEN NOT MATCHED THEN INSERT 
			   ([NAME]
			   ,[MANUFACTURER]
			   ,[BRAND]
			   ,[CATEGORY]
			   ,[SUBCATEGORY]
			   ,[CONVERSION_PACK]
			   ,[CREATED_DATE]
			   ,[CREATED_BY])
		 VALUES (
			   src.NAME
			   ,src.MANUFACTURER
			   ,src.BRAND
			   ,src.CATEGORY
			   ,src.SUBCATEGORY
			   ,src.NEW_CONVERSION_PACK
			   ,GETDATE()
			   ,CURRENT_USER );

	INSERT INTO [off].[IN_FOREIGN_PRODUCT_GOLD_IMPORT_HIST]
           ([F_PRODUCT_KEY]
           ,[NAME]
           ,[MANUFACTURER]
           ,[BRAND]
           ,[CATEGORY]
		   ,[SUBCATEGORY]
		   ,[CONVERSION_PACK]
           ,[JOB_ID]
           ,[FILENAME]
           ,[CREATE_DATE]
           ,[CREATED_BY])
     SELECT 
			F_PRODUCT_KEY
           ,NAME
           ,MANUFACTURER
           ,BRAND
           ,CATEGORY
		   ,SUBCATEGORY
		   ,CONVERSION_PACK
           ,JOB_ID
           ,FILENAME
           ,GETDATE() CREATE_DATE
           ,CREATED_BY
	FROM [off].[IN_FOREIGN_PRODUCT_GOLD_IMPORT]
	WHERE NAME IS NOT NULL;

	END;

ELSE IF @inPhase = 'GOLD REVIEW MATCH'
	BEGIN 
		INSERT INTO [off].TL_DIM_SRC_GOLD_PRODUCT_MAP (
			NAME,
			MANUFACTURER,
			BRAND,
			CATEGORY,
			SUBCATEGORY,
			CONVERSION_PACK,
			SOURCE_SYSTEM_KEY,
			GOLD_F_PRODUCT_KEY,
			GOLD_PRODUCT_NAME,
			PROCESS_STATUS,
			CREATED_DATETIME,
			CREATED_BY,
			JOB_RUN_ID
		)
		select distinct
			mr.NAME,
			p.MANUFACTURER,
			p.BRAND,
			p.CATEGORY,
			p.SUBCATEGORY,
			p.CONVERSION_PACK,
			mr.SOURCE_SYSTEM_KEY,
			p.F_PRODUCT_KEY AS GOLD_F_PRODUCT_KEY,
			p.NAME AS GOLD_PRODUCT_NAME,
			'Manual Review' as PROCESS_STATUS,
			GETDATE() AS CREATED_DATETIME,
			CURRENT_USER AS CREATED_BY,
			@inJobID AS JOB_RUN_ID
		FROM [off].[IN_FOREIGN_PRODUCT_MANUAL_REVIEW_IMPORT] mr
		JOIN [off].TL_DIM_FOREIGN_PRODUCT p
			on mr.PROPOSAL_NAME = p.NAME
		WHERE 1=1  and (mr.RESULT = 'GOLD' or (mr.RESULT != 'PROPOSAL'  AND mr.PROPOSAL_NAME != 'N/A') )
		AND mr.JOB_ID = @inJobID
		and not exists ( select 1 from [off].TL_DIM_SRC_GOLD_PRODUCT_MAP map where map.NAME = mr.NAME );


	INSERT INTO [off].[IN_FOREIGN_PRODUCT_MANUAL_REVIEW_IMPORT_HIST]
           ([GROUP_ID]
           ,[NAME]
           ,[NORM_NAME]
           ,[MANUFACTURER]
           ,[BRAND]
           ,[CATEGORY]
		   ,[CONVERSION_PACK]
           ,[PRODUCT_KEY]
           ,[SOURCE_SYSTEM_KEY]
           ,[PROPOSAL_NAME]
           ,[PROPOSAL_F_PRODUCT_KEY]
           ,[MATCH_SCORE]
           ,[CONFIDENCE_SCORE]
           ,[RESULT]
           ,[PROCESS_STATUS]
           ,[JOB_ID]
           ,[FILENAME]
           ,[CREATE_DATE]
           ,[CREATED_BY])
     SELECT
            [GROUP_ID]
           ,[NAME]
           ,[NORM_NAME]
           ,[MANUFACTURER]
           ,[BRAND]
           ,[CATEGORY]
		   ,[CONVERSION_PACK]
           ,[PRODUCT_KEY]
           ,[SOURCE_SYSTEM_KEY]
           ,[PROPOSAL_NAME]
           ,[PROPOSAL_F_PRODUCT_KEY]
           ,[MATCH_SCORE]
           ,[CONFIDENCE_SCORE]
           ,[RESULT]
           ,[PROCESS_STATUS]
           ,[JOB_ID]
           ,[FILENAME]
           ,GETDATE()
           ,[CREATED_BY]
		FROM [off].[IN_FOREIGN_PRODUCT_MANUAL_REVIEW_IMPORT]
		WHERE JOB_ID = @inJobID;



	END;

	ELSE IF @inPhase = 'CALC MONTHS'
	BEGIN 
		EXECUTE [off].[CALC_GOLD_PRODUCT_DATA] @inJobID;
	END;
END;


