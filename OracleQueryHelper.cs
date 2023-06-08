using OracleHelpers.Utils;
using System;
using System.Collections.Generic;

namespace OracleHelpers.Helpers
{
    public class OracleQueryHelper : Disposer
    {
        public string SelectByItem(string tablename, string columnname, string item)
        {
            return $"select * from {tablename} where {columnname} like '{item}' ";
        }

        public string SelectBatchRecordsByStatusIDcondtion(string tablename, string columnname, UInt64 item)
        {
            return $"select * from {tablename} where {columnname} = {item}  and status_id not in(5) ";
        }

        public string SelectNextValue(string columnName, string tableName)
        {
            return $"select {columnName} from {tableName}";
        }

        public string SelectRightsRecord(string contractNo, string programmeNo, string startDate, string endDate, string platform)
        {
            return $"select * from V_VOD_PROGRAM_RIGHTS t where t.ctr_contrt_num = {contractNo} and t.prg_prog_num = {programmeNo} and cast(t.start_date as date) >= '{startDate}'" +
                $"and cast(trunc(t.end_date) as date) <= '{endDate}' and t.services like '{platform}'";
        }

        public string SelectEpisode(string contractNo, string programmeNo)
        {
            return $"select PRG_SYB.PRG_TITLE_TEX, PRG_SYB.PRG_PROG_NUM, PRG_SYB.Prg_Auk, V_PROJECT.ctr_contrt_num from PRG_SYB " +
                $" INNER JOIN V_PROJECT ON PRG_SYB.CTR_KEY = V_PROJECT.ctr_key " +
                $" where V_PROJECT.ctr_contrt_num = {contractNo} and PRG_SYB.PRG_PROG_NUM = {programmeNo}";
        }

        public string SelectRightsTermsRules(int rightsTermId)
        {
            return $"Select cr.trigger_type, cr.name, nvl(v.value, '1') VALUE, cr.description,t.rights_term_id from RIGHTS_TERM_RULES t " +
                $" join custom_rules cr on t.custom_rule_id = cr.id " +
                $" left outer join custom_rule_expressions e on cr.rule_expression_id = e.parent_id " +
                $" left outer join custom_rule_parameter_values v on e.value = v.id " +
                $" where t.rights_term_id = {rightsTermId} " +
                $" order by 1";
        }

        public string SelectEEPTriggerEvent(char eventEntityRowState)
        {
            return $"select * from EEP_TRIGGER_EVENT t " +
                $" where t.event_entity_row_state like '{eventEntityRowState}' " +
                $" and t.last_updated_user_id like 'SVCEEPLISTENER' " +
                $" and t.event_data is not null " +
                $" and rownum = 1";
        }

        public string InsertNewProgramme(string tablename, int programmeNo, int programmeKeyNo)
        {
            return $"insert into {tablename} (ctr_key, prg_prog_num, u_version, prg_auk, prjdet_projct_num, ctr_item_seq_num, prg_episode_num, prg_type_cod, prg_title_tex, prg_prod_placement_flg)" +
                $"values(1707314291, {programmeNo}, '!', {programmeKeyNo}, 1707313792, 1, {programmeNo}, 'C', 'KING OF QUEENS YR 1 - PILOT', 'N')";
        }

        public string DeleteRecord(string tableName, string item, string value, bool valueIsNumber)
        {
            if (valueIsNumber)
            {
                return $"delete from {tableName} where {item} = {Convert.ToInt32(value)}";
            }
            else
            {
                return $"delete from {tableName} where {item} like '{value}'";
            }
        }

        public string SelectCliItemDescText(int id, string variantId)
        {
            return $"select cli.cli_item_desc_tex from mm_item join cli on MM_ITEM.MM_ITEM_STATUS_CLI_KEY = cli.cli_key where mm_item.mm_item_key= {id} and variant_id='{variantId}'";
        }

        public string UpdateRecordCreateTrigger(string newStatus, int mmItemKey, string variantId)
        {
            return $"Update MM_Item set MM_ITEM.MM_ITEM_STATUS_CLI_KEY=(select t.cli_key from CLI t where t.cli_title_cod = 'MMIST' and t.cli_item_desc_tex ='{newStatus}') where mm_item_key={mmItemKey} and variant_id='{variantId}'";
        }
        public string MediaItemForVariantId(string variantId)
        {
            return $"select  ma.mm_act_audio_desc_cod as audioDescription, mi.variant_id as VariantId, mi.MM_ITEM_STATE_NAM as ItemCurrentState,ast.code as AssetType, MI.mm_item_nam as ItemTitle," +
                   $"mf.MM_FILE_ID_COD as mediaId, ma.mm_act_afd as afdRatio, (select c.cli_item_nam from CLI c where c.cli_key = ma.MM_ACT_LINES_CLI_KEY) as definition, " +
                   $"ma.mm_act_frames_rate_num as frameRate, (select c.cli_item_desc_tex from CLI c where c.cli_key =ma.mm_act_prot_ratio_cli_key) as protectedRatio, " +
                   $"(select c.cli_item_nam from CLI c where c.cli_key = ma.MM_ACT_AFD_RATIO_CLI_KEY) as afdCode, " +
                   $"(select c.cli_item_nam from CLI c where c.cli_key = ma.MM_ACT_RATIO_CLI_KEY) as aspectRatio, " +
                   $" (select c.cli_item_nam from CLI c where c.cli_key = ma.mm_act_aspect_cli_key) as aspectType," +
                   $" (select c.cli_item_desc_tex from CLI c where c.cli_key = mi.mm_item_type_cli_key) as ItemType," +
                   $"  mt.mm_time_material_id as materialId, mt.mm_time_som_cod as som, mt.mm_time_eom_cod as eom," +
                   $"  (select c.cli_item_nam from CLI c where c.cli_key = (select v.vers_progman_cli_key from vers v where vers_key = mi.vers_key)) as ProgrammeManagerVersion," +
                   $"   mt.mm_time_duration_cod as duration from MM_ITEM mi" +
                   $"  join MEDIA_ITEM_AND_ASSET_TYPES mitem on mitem.cli_key = mi.MM_ITEM_TYPE_CLI_KEY" +
                   $"  join asset_type ast on mitem.asset_type_id = ast.id" +
                   $"  join MM_FILE mf on mf.MM_FILE_KEY = mi.MM_FILE_KEY" +
                   $" join MM_TIME mt on mt.MM_ITEM_KEY = mi.mm_item_key" +
                   $"  join MM_ACT ma on ma.mm_act_key = mt.mm_act_key" +
                   $"  join dls_media_loc_and_type dmt on mf.mm_file_id_cod = dmt.mm_file_id_cod" +
                   $"  where dmt.type_of_media in ('MSPDAM', 'L2V') and dmt.active_flag = 'Y' and mt.mm_time_material_id is not null and mi.variant_id =  '{variantId}'";
        }


        //Data migration job tables queries

        public string SelectColoumnName(string tablename)
        {
            return $"select column_name  FROM USER_TAB_COLUMNS where table_name= UPPER('{tablename}')";
        }



        public string SelectMetaDataDataByEntityIds(List<string> item)
        {
            var inid = string.Join(",", item);
            return $"select 'brand' entityType, a.id externalid,a.description,c.tags || c.weighting tags,c4.title,sky.title skytitle,c4.title_sort_name sortTitle,c4.summary_long long_synopsis,c4.description short_synopsis,pt.code programme_type,"
            + "case pb.code when 'FILM4' then 'F4' when 'CHANNEL4' then 'C4' when '4MUSIC' then '4M' when 'MORE4' then 'M4' when 'E4' then 'E4' when 'ALL4' then 'ALL4'"
            + "end presentation, c.genre_enum_code genre, F_GET_GENRE_SKY_YV(SKY.ID, 0) skygenre, F_GET_GENRE_SKY_YV(SKY.ID, 1) skygenre,"
            + "F_GET_GENRE_SKY_YV(SKY.ID, 2) skygenre, F_GET_GENRE_SKY_YV(SKY.ID, 3) skygenre,F_GET_GENRE_SKY_YV(SKY.ID, 4) skygenre,F_GET_GENRE_SKY_YV(yv.ID, 0) yvgenre,F_GET_GENRE_SKY_YV(yv.ID, 1) yvgenre,"
            + "F_GET_GENRE_SKY_YV(yv.ID, 2) yvgenre,F_GET_GENRE_SKY_YV(yv.ID, 3) yvgenre,F_GET_GENRE_SKY_YV(yv.ID, 4) yvgenre,c.episode_order_type displayorder,"
            + "c.brand_family,c.sub_genre,c.collection from asset a join data_migration_content_data c on a.id = c.brand_id left join ASSET_METADATA c4 on c4.asset_id = a.id "
            + "and c4.asset_metadata_type_id = 2722671 and c4.editorially_approved_flag = 'Y' left join programme_type pt on pt.id = c4.programme_type_id left join ASSET_METADATA_PRES_BRAND prb "
            + "on prb.asset_metadata_id = c4.id and prb.exclude_flag = 'N' left join presentation_brand pb on pb.id = prb.presentation_brand_id left join ASSET_METADATA sky on sky.asset_id = a.id and sky.asset_metadata_type_id = 2722677 and sky.editorially_approved_flag = 'Y'"
            + "left join ASSET_METADATA yv on yv.asset_id = a.id and yv.asset_metadata_type_id = 2722730 and yv.editorially_approved_flag = 'Y' where a.asset_type_id = 3"
            + $"and a.id in ({inid}) order by externalid asc";
        }
        public string SelectTagsBrandMetaData(int id)
        {
            return $"select (Tags || '-' || weighting)as tags from data_migration_content_data_keyword where brand_id ={id}";

        }

        public string SelectBrandMetaDataDataByEntityIds(List<string> item)
        {
            var inid = string.Join(",", item);
            return $"select 'brand' entityType,  a.id externalid,  a.description, c4.title, sky.title skytitle, c4.title_sort_name sortTitle, c4.summary_long long_synopsis, c4.description short_synopsis,  pt.code programme_type, case pb.code   "
                + "when 'FILM4' then 'F4'  when 'CHANNEL4' then 'C4' when '4MUSIC' then '4M' when 'MORE4' then 'M4' when 'E4' then 'E4'  when 'ALL4' then 'ALL4' end presentation, c.genre_enum_code genre, "
                + "case when c.episode_order_type='asc' then 'First Series First Episode' "
                + "when c.episode_order_type='desc' then 'Latest Series Latest Episode'  "
                + " when c.episode_order_type='ascesc' then 'Latest Series First Episode'  end displayorder,  c.brand_family, c.sub_genre, c.collection strand "
                + " from asset a "
                + " join data_migration_content_data c "
                + " on a.id = c.brand_id "
                + " left join flat_asset_metadata c4 "
                + " on c4.asset_id = a.id "
                + " and c4.asset_metadata_type_id = 2722671 "
                + " and c4.editorially_approved_flag = 'Y' "
                + "  left join programme_type pt "
                + "  on pt.id = c4.programme_type_id "
                + "   left join ASSET_METADATA_PRES_BRAND prb"
                + "    on prb.asset_metadata_id = c4.id "
                + "   and prb.exclude_flag = 'N' "
                + "   left join presentation_brand pb "
                + "  on pb.id = prb.presentation_brand_id "
                + "   left join flat_asset_metadata sky "
                + "   on sky.asset_id = a.id "
                + "    and sky.asset_metadata_type_id = 2722677 "
                + "   and sky.editorially_approved_flag = 'Y' "
                + "   left join flat_asset_metadata yv "
                + "    on yv.asset_id = a.id "
                + "   and yv.asset_metadata_type_id = 2722730 "
                + "   and yv.editorially_approved_flag = 'Y' "
                + "   where a.asset_type_id = 3 "
                + $" and a.id in ({inid}) order by externalid asc ";
        }
        public string SelectFlatAssetIdsBrandMetaData(int id)
        {

            return "   select p.code,c.genre_enum_code, a.* from " +
                   "   flat_asset_metadata a " +
                   "   join programme_type p on p.id=a.programme_type_id " +
                   "   join data_migration_content_data c on c.brand_id=a.asset_id " +
                   $"   where a.asset_id={id} and a.asset_metadata_type_id in(2722671,2722677,2722730) ";


        }
        public string SelectGenreCodeFeatureMetaData(List<int> ids)
        {
            var inid = string.Join(",", ids);
            return "   select gm.genre_enum_code, amt.short_description from flat_asset_metadata a " +
                   "   join flat_metadata_genre_type amg on amg.asset_metadata_id=a.id " +
                   "   join asset_metadata_type amt on amt.id=a.asset_metadata_type_id " +
                   "   join GENRE_TYPE gt on gt.id=amg.genre_type_id " +
                   "   join genre_mapping gm on gm.genre_type_id=gt.id " +
                  $"    where a.asset_id in ({inid})";

        }
        public string SelectGenreCodeBrandMetaData(List<int> ids)
        {
            var inid = string.Join(",", ids);
            return "   select g.* from flat_metadata_genre_type  a " +
                  $"   join genre_mapping g on g.genre_type_id=a.genre_type_id where a.asset_metadata_id in ({inid})";

        }
        public string SelectTagsWeightBrandMetaData(int id)
        {
            return $" select (Tags || '-' || weighting)as tags from data_migration_content_data_keyword where brand_id= {id}";

        }


        public string SelectSeriesAll()
        {
            return $" SELECT a.Id FROM Asset a WHERE a.asset_type_id = 1 AND a.deleted_flag = 'N' " +
                   $" AND to_char(a.ID) not in " +
                   $" (SELECT entity_id FROM data_migration_job) " +
                   $" AND (SELECT COUNT(*) " +
                   $" FROM asset epa" +
                   $" JOIN asset_parent ap " +
                   $" ON epa.id = ap.asset_id " +
                   $" WHERE ap.asset_parent_id = a.id) > 1";
        }
        
        public string SelectSeriesofRemaining(List<int> ctrNumber)
        {
            var inid = string.Join(",", ctrNumber);
            return $"   SELECT SE.ID  AS SERIES_ID, " +
                   $" SE.CTR_CONTRT_NUM, " +
                   $" SE.Description SERIES_DESCRIPTION , " +
                   $" BR.ID   AS BRANDID " +
                   $" FROM ASSET SE " +
                   $" LEFT JOIN ASSET_PARENT APX " +
                   $" ON APX.ASSET_ID = SE.ID " +
                   $" LEFT JOIN ASSET BR " +
                   $" ON APX.ASSET_PARENT_ID = BR.ID " +
                   $" WHERE SE.ASSET_TYPE_ID = 1 " +
                   $" and (SELECT COUNT(*) " +
                   $" FROM ASSET EPA " +
                   $" JOIN ASSET_PARENT AP " +
                   $" ON EPA.ID = AP.ASSET_ID " +
                   $" WHERE AP.ASSET_PARENT_ID = SE.ID) > 1 " +
                   $" and SE.CTR_CONTRT_NUM in ({inid}) " +
                   $" order by se.CTR_CONTRT_NUM ";
        }

        public string SelectGetNameByIDS(List<UInt64> ids)
        {
            var inid = string.Join(",", ids);
            return $"select ID,DESCRIPTION  FROM asset where id in ({inid}) order by id asc";
        }

        public string SelectBrands()
        {
            return $"select ID from asset where asset_type_id = 3 AND deleted_flag = 'N'";

        }
        public string SelectSeriesBritboxApplePlatform()
        {
            return "select *" +
                " from (select distinct se.ctr_contrt_num as SE_Ctr," +
                " se.id as Series," +
                " se.external_title as Se_Title," +
                " se.parent_asset_type_id as SE_ParentType," +
                " se.asset_type_id as SE_Type," +
                " (select COUNT(*) from asset epa" +
                " join asset_parent ap on epa.id = ap.asset_id" +
                " where ap.asset_parent_id = se.id) TOTAL_COUNT " +
                " from asset a" +
                " join asset_parent ap on a.id = ap.asset_id" +
                " join asset se on ap.asset_parent_id = se.id " +
                " left join asset_parent apx on apx.asset_id = se.id " +
                " join asset_metadata am on am.asset_id = a.id " +
                " left join job_item ji on ji.asset_metadata_id = am.id " +
                " where se.asset_type_id = 1 " +
                " and se.deleted_flag = 'N' " +
                " and ji.provider_platform_id in (5, 20, 97) " +
                " order by se.ctr_contrt_num) z " +
                " where z.total_count > 1";
        }
        public string SelectFeatureBritboxApplePlatform()
        {
            return "select *" +
                "  from(select distinct se.ctr_contrt_num," +
                "  se.id as Series_Id," +
                "  se.external_title as Se_Title," +
                "  se.parent_asset_type_id as SE_ParentType," +
                "  se.asset_type_id as SE_Type," +
                "  (select COUNT(*)" +
                "  from asset epa" +
                "  join asset_parent ap" +
                "  on epa.id = ap.asset_id" +
                "  where ap.asset_parent_id = se.id) TOTAL_COUNT" +
                "  from  asset a" +
                "  join asset_parent ap on a.id = ap.asset_id" +
                "  JOIN asset se on ap.asset_parent_id = se.id" +
                "  left join asset_parent apx" +
                "  on apx.asset_id = se.id" +
                "  join asset_metadata am on am.asset_id = a.id" +
                "  left join job_item ji on ji.asset_metadata_id = am.id" +
                "  where se.asset_type_id = 1" +
                "  and se.deleted_flag = 'N'" +
                "  and ji.provider_platform_id in (5, 20, 97)" +
                "  order by se.ctr_contrt_num) z" +
                "  where z.total_count = 1";
        }
        public string SelectAssetMaterialIdBritboxApplePlatform()
        {
            return " SELECT distinct MI1.VARIANT_ID, MI1.MM_ITEM_ID_COD, C.CLI_ITEM_NAM, CLI1.CLI_ITEM_NAM as CLI_ITEMNAM , MF.MM_FILE_ID_COD, D.TYPE_OF_MEDIA " +
                " FROM mm_item mi1 " +
                " JOIN MM_FILE MF " +
                " ON MF.MM_FILE_KEY = MI1.MM_FILE_KEY " +
                " JOIN dls_media_loc_and_type D " +
                " ON D.MM_FILE_ID_COD = MF.MM_FILE_ID_COD " +
                " left join vers v " +
                " on v.vers_key = mi1.vers_key " +
                " left join cli c " +
                " on c.cli_key = v.vers_progman_cli_key " +
                " JOIN CLI CLI1 " +
                " ON CLI1.CLI_KEY = mi1.mm_item_status_cli_key " +
                " WHERE mi1.mm_item_status_cli_key = 267552656 " +
                " AND D.TYPE_OF_MEDIA = 'MSPDAM' " +
                " and mf.mm_file_id_cod = 'PFTM1' " +
                " and mi1.mm_item_present_flg = 'Y' " +
                " AND MI1.VARIANT_ID IN(select distinct mi.variant_id " +
                " from media_lock ml " +
                " join mm_item mi " +
                " on mi.mm_item_key = ml.mm_item_key " +
                " join job_item ji " +
                " on ji.id = ml.job_item_id " +
                " where ji.Provider_Platform_id in (5, 20, 97) " +
                " AND MI.VARIANT_ID IS NOT NULL) ";

        }
        public string SelectEpisodeBritboxApplePlatform()
        {
            return "select *" +
                " from" +
                " (select distinct a.ctr_contrt_num, a.prg_prog_num," +
                "  a.id as EP_id, a.parent_asset_type_id," +
                "  (select COUNT(*)" +
                "  from asset epa" +
                "  join asset_parent ap" +
                "  on epa.id = ap.asset_id" +
                "  where ap.asset_parent_id = se.id) TOTAL_COUNT" +
                "  from asset a" +
                "  join asset_parent ap on a.id = ap.asset_id " +
                "  join asset se on ap.asset_parent_id = se.id " +
                "  left join asset_parent apx on apx.asset_id = se.id " +
                "  join asset_metadata am on am.asset_id = a.id " +
                "  left join job_item ji on ji.asset_metadata_id = am.id " +
                "  where a.asset_type_id = 2 " +
                "  and a.deleted_flag = 'N' " +
                "  and ji.provider_platform_id in (5, 20, 97) " +
                "  order by a.ctr_contrt_num,a.prg_prog_num) z " +
                "  where z.total_count > 1 ";
        }
        public string SelectFeature(List<string> ids)
        {
            var inid = string.Join(",", ids);
            return $"select * " +
                   $" from (select asset_id, " +
                   $" ctr_contrt_num as ContractNumber, " +
                   $" ctr_contrt_num || '-' || lpad(EP_No, 3, '0') as CtrPrg, " +
                   $" Series_Id, " +
                   $" Total_Count as NoOfEpisodes, " +
                   $" Feature_Description, " +
                   $" series_desctiption, " +
                   $" BrandId, " +
                   $" EP_No " +
                   $" from (select apx.asset_parent_id asset_id, " +
                   $" apx.asset_parent_id, " +
                   $" se.ctr_contrt_num, " +
                   $" se.id as Series_Id, " +
                   $" br.id as BrandId, " +
                   $" (select epa.description " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id)  as Feature_Description, " +
                   $" se.description series_desctiption, " +
                   $" br.external_title, " +
                   $" se.parent_asset_type_id, " +
                   $" se.movie_flag, " +
                   $" (select max(epa.prg_prog_num) " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id) as EP_No, " +
                   $" (select COUNT(*) " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id) TOTAL_COUNT " +
                   $" from asset se " +
                   $" left join asset_parent apx " +
                   $" on apx.asset_id = se.id " +
                   $" left join asset br " +
                   $" on apx.asset_parent_id = br.id " +
                   $" where se.asset_type_id = 1) z " +
                   $" where z.total_count = 1) " +
                   $" where CtrPrg in ({inid}) order by CtrPrg  asc ";

        }
        public string SelectAllFeature(string ids)
        {
            string Items = "'" + ids + "'";
            return $"select * " +
                   $" from (select asset_id, " +
                   $" ctr_contrt_num as ContractNumber, " +
                   $" ctr_contrt_num || '-' || lpad(EP_No, 3, '0') as CtrPrg, " +
                   $" Series_Id, " +
                   $" Total_Count as NoOfEpisodes, " +
                   $" Feature_Description, " +
                   $" series_desctiption, " +
                   $" BrandId, " +
                   $" EP_No " +
                   $" from (select apx.asset_parent_id asset_id, " +
                   $" apx.asset_parent_id, " +
                   $" se.ctr_contrt_num, " +
                   $" se.id as Series_Id, " +
                   $" br.id as BrandId, " +
                   $" (select epa.description " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id)  as Feature_Description, " +
                   $" se.description series_desctiption, " +
                   $" br.external_title, " +
                   $" se.parent_asset_type_id, " +
                   $" se.movie_flag, " +
                   $" (select max(epa.prg_prog_num) " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id) as EP_No, " +
                   $" (select COUNT(*) " +
                   $" from asset epa " +
                   $" join asset_parent ap " +
                   $" on epa.id = ap.asset_id " +
                   $" where ap.asset_parent_id = se.id) TOTAL_COUNT " +
                   $" from asset se " +
                   $" left join asset_parent apx " +
                   $" on apx.asset_id = se.id " +
                   $" left join asset br " +
                   $" on apx.asset_parent_id = br.id " +
                   $" where se.asset_type_id = 1) z " +
                   $" where z.total_count = 1) " +
                   $" where CtrPrg in ({Items}) order by CtrPrg  asc ";

        }
        public string SelectFeatureAll()
        {
            return $"SELECT a.Id FROM Asset a WHERE a.asset_type_id = 1 AND a.deleted_flag = 'N' " +
                   $" AND to_char(a.ID) not in " +
                   $"(SELECT entity_id FROM data_migration_job) " +
                   $" AND (SELECT COUNT(*) " +
                   $"FROM asset epa " +
                   $" JOIN asset_parent ap " +
                   $"ON epa.id = ap.asset_id " +
                   $" WHERE ap.asset_parent_id = a.id) = 1 ";
        }
        public string SelectJulyFeature(List<string> ids)
        {

            return $" select a.* from asset a where a.ctr_contrt_num " +
                  $" ctr_contrt_num||'-'||lpad(EP_No,3,'0') as CtrPrg, " +
                  $" Total_Count as NoOfEpisodes,Feature_Description,BrandId " +
                  $" from (select apx.asset_id, apx.asset_parent_id, se.ctr_contrt_num, se.id as \"Series_Id\", br.id as BrandId, se.description as Feature_Description, br.external_title, se.parent_asset_type_id, se.movie_flag, (select max(epa.prg_prog_num) from asset epa join asset_parent ap " +
                  $" on epa.id = ap.asset_id where ap.asset_parent_id = se.id) as EP_No, (select COUNT(*) from asset epa join asset_parent ap on epa.id = ap.asset_id where ap.asset_parent_id = se.id) TOTAL_COUNT from asset se" +
                  $" left join asset_parent apx on apx.asset_id = se.id left join asset br on apx.asset_parent_id = br.id where se.asset_type_id = 1   order by se.ctr_contrt_num) z where z.total_count =1";
        }

        public string SelectEpisodeAll()
        {
            return $"SELECT DISTINCT a.id " +
                    $"FROM asset a " +
                    $"JOIN asset_parent ap " +
                    $"ON a.id = ap.asset_id " +
                    $"JOIN asset se " +
                    $"ON ap.asset_parent_id = se.id " +
                    $"LEFT JOIN asset_parent apx " +
                    $"ON apx.asset_id = se.id " +
                    $"WHERE a.asset_type_id = 2 " +
                    $"AND a.deleted_flag = 'N' " +
                    $"AND to_char(a.ID) not in " +
                    $"(SELECT entity_id FROM data_migration_job) " +
                    $"and (SELECT COUNT(*) " +
                    $"FROM asset epa " +
                    $"JOIN asset_parent ap " +
                    $"ON epa.id = ap.asset_id " +
                    $"WHERE ap.asset_parent_id = se.id)>1 ";
        }
        public string SelectEpisodeMetaDataAll(string inid)
        {
            return $"select ctr_contrt_num as ContractNumber,Total_Count as NoOfEpisodes " +
                   $"from (select apx.asset_id, apx.asset_parent_id, se.ctr_contrt_num,se.id as \"Series_Id\", br.id as  " +
                   $" BrandId,  br.external_title, se.parent_asset_type_id, se.movie_flag, (select COUNT(*) from asset epa  join asset_parent ap   " +
                   $" on epa.id = ap.asset_id where ap.asset_parent_id = se.id) TOTAL_COUNT  from asset se " +
                   $" left join asset_parent apx  on apx.asset_id = se.id left join asset br on apx.asset_parent_id = br.id where " +
                   $" se.asset_type_id = 1  order by se.ctr_contrt_num) z " +
                   $"where z.ctr_contrt_num in ({inid})";


        }
        public string SelectFreeWheelFlag(string ctrNumber, string prgNumber)
        {
            return $"select t.AD_CERT_RATING  from TAPS_AD_CERTIFICATION t where t.ctr_contrt_num ={ctrNumber} and t.prg_prog_num={prgNumber} ";
                
        }
        public string SelectPlacementFlag(string ctrNumber, string prgNumber)
        {
            return $"select c.ctr_contrt_num,p.prg_prog_num, p.prg_prod_placement_flg, " +
                    $"p.prg_prod_placement_notes  " +
                    $"from prg_syb p " +
                    $"join ctr_syb c on c.ctr_key = p.ctr_key  " +
                    $"where c.ctr_contrt_num = {ctrNumber} and p.prg_prog_num ={prgNumber} ";
        }
        public string SelectFeatureLevelMetadata(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";
            return $" select a.id,  ep.description Name, ep.prg_prog_num EpisodeNumber, " +
                    $" bm.Title, epa.sequence_number RunningOrder, case when ep.movie_flag='Y' then 'Film' else 'Standalone' end ContentType,  " +
                    $" bm.description ShortSynopsys, v.description ExtraSynopsys, bm.summary_long LongSynopsys,epa.significant_date first_tx_date, " +
                    $" ep.movie_flag,b.id BrandID,c.brand_family, c.genre_enum_code Genre,c.collection strand, " +
                    $" case when pb.code='CHANNEL4'  then 'C4' " +
                    $"when pb.code='4MUSIC'  then '4M' " +
                    $"when pb.code='MORE4'  then 'M4' " +
                    $"when pb.code='FILM4'  then 'F4' " +
                    $"when pb.code='E4'  then 'E4' " +
                    $"when pb.code='ALL4'  then 'ALL4' " +
                    $"end  presentation " +
                    $",py.name YoutubeMatchPolicy , t.AD_CERT_RATING FreeWheelCertification, fw.freewheelcategory, fw.supplier " +
                    $"from Asset a " +
                    $"left join  flat_asset_metadata f  on a.id=f.asset_id  and f.asset_metadata_type_id=2722671 " +
                    $"left join  flat_asset_metadata yt  on a.id=yt.asset_id  and yt.asset_metadata_type_id=2722675 " +
                    $"left join policy  p on p.id=yt.policy_id " +
                    $"left join asset_parent ap on ap.asset_id=a.id " +
                    $"left join asset b on b.id=ap.asset_parent_id " +
                    $"left join  flat_asset_metadata bm  on b.id=bm.asset_id  and bm.asset_metadata_type_id=2722671 " +
                    $"left join  flat_asset_metadata v  on b.id=v.asset_id  and v.asset_metadata_type_id=2722669 " +
                    $"left join flat_metadata_pres_brand fpb on fpb.asset_metadata_id=bm.id " +
                    $"left join presentation_brand pb on pb.id=fpb.presentation_brand_id " +
                    $"left join data_migration_content_data c on c.brand_id=b.id " +
                    $"left join asset ep on ep.ctr_contrt_num=a.ctr_contrt_num and ep.asset_type_id=2 " +
                    $"left join  flat_asset_metadata epa  on ep.id=epa.asset_id  and epa.asset_metadata_type_id=2722671 and epa.editorially_approved_flag='Y' " +
                    $"left join asset se on se.ctr_contrt_num=a.ctr_contrt_num and se.asset_type_id=1 " +
                    $"left join  flat_asset_metadata fmsey  on se.id=fmsey.asset_id  and fmsey.asset_metadata_type_id=2722675 " +
                    $"left join policy py on py.id=fmsey.match_policy_id " +
                    $"left join TAPS_AD_CERTIFICATION t on t.ctr_contrt_num=a.ctr_contrt_num " +
                    $"left join (select distinct c.ctr_contrt_num, ca.cca_contrt_catgry_nam as FreeWheelCategory, s.sup_suplr_nam as Supplier from ait a " +
                    $"join ctr_syb c on a.ctr_key = c.ctr_key " +
                    $"join cca ca on a.cca_contrt_catgry_cod = ca.cca_contrt_catgry_cod " +
                    $" join app_pirate.agr ag on a.prjdet_projct_num = ag.prjdet_projct_num and a.agr_agrmnt_num = ag.agr_agrmnt_num " +
                    $"join app_pirate.rol r on ag.role_key = r.role_key " +
                    $"join app_pirate.sup s on r.sup_suplr_key = s.sup_suplr_key  " +
                    $"and a.class_cod not in ('D', 'X')) fw on fw.ctr_contrt_num=a.ctr_contrt_num " +
                    $"where a.ctr_contrt_num in ({Items}) and a.asset_type_id=1  ";
                    
        }
        public string SelectFreeWheelCategory(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";
            return  $" select c.ctr_contrt_num, ca.cca_contrt_catgry_nam as FreeWheelCategory, " +
                    $" s.sup_suplr_nam as Supplier, a.* from ait a  " +
                    $" join ctr_syb c on a.ctr_key = c.ctr_key " +
                    $" join cca ca on a.cca_contrt_catgry_cod = ca.cca_contrt_catgry_cod " +
                    $" join agr ag on a.prjdet_projct_num = ag.prjdet_projct_num and " +
                    $" a.agr_agrmnt_num = ag.agr_agrmnt_num " +
                    $" join rol r on ag.role_key = r.role_key " +
                    $" where c.ctr_contrt_num in ({Items})  " +
                    $" and a.class_cod not in ('D', 'X') ";
        }
        public string SelectSupplierValue(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";

            return $" select  c.ctr_contrt_num, ca.cca_contrt_catgry_nam category, s.sup_suplr_nam supplier, a.*   from ait a " +
                   $" join ctr_syb c on a.ctr_key = c.ctr_key join cca ca on a.cca_contrt_catgry_cod = ca.cca_contrt_catgry_cod  " +
                   $" join agr ag on a.prjdet_projct_num = ag.prjdet_projct_num and a.agr_agrmnt_num = ag.agr_agrmnt_num join rol r on ag.role_key = r.role_key " +
                   $" join sup s on r.sup_suplr_key = s.sup_suplr_key where c.ctr_contrt_num in ({Items}) " +
                   $" and a.class_cod not in ('D', 'X')   " +
                   $"  order by a.ait_key FETCH FIRST 1 ROWS ONLY ";
        }
        public string SelectSourceValue(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";
            
            return $" select a.ctr_contrt_num ,cpi.acquisition_ind, cpi.start_date, cpi.end_date from asset a " +
                   $"inner join asset_parent ap on a.id = ap.asset_parent_id " +
                   $"inner join asset ep on ap.asset_id = ep.id " +
                   $"inner join content_plan_item cpi on ep.id = cpi.asset_id " +                     
                   $" where a.ctr_contrt_num in ({Items}) order by cpi.start_date desc fetch first 1 row only ";
        }
        public string SelectDeptValue(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";

            return $" select d.cdt_dept_cod " +
                   $" from ait a " +
                   $" join ced c on a.ced_editor_cod = c.ced_editor_cod " +
                   $" join cdt d on c.cdt_dept_cod = d.cdt_dept_cod " +
                   $" join ctr_syb ct on a.ctr_key = ct.ctr_key " +
                   $" where  ct.ctr_contrt_num in ({Items}) and  a.class_cod<>'D'  " +
                   $" order by a.ait_key " +
                   $" fetch first 1 row only " ;
        }
        public string SelectEpisode(List<string> ids)
        {
            var inid = string.Join(",", ids);
            return $"  select a.id, 'title' EntityType, 'episode' titletype, " +
                  $" REPLACE(a.unique_reference_id, '/', '-') as unique_reference_id," +
                  $" a.description, a.prg_prog_num, a.ctr_contrt_num, b.id seriesId, b2.id brandid " +
                  $" from Asset a " +
                  $" left join asset_parent p on a.id = p.asset_id " +
                  $" left join asset b on b.id = p.asset_parent_id " +
                  $" left join asset_parent p2 on b.id = p2.asset_id " +
                  $" left join asset b2 on b2.id = p2.asset_parent_id " +
                  $" where a.asset_type_id = 2 and a.deleted_flag = 'N' " +
                  $" and a.unique_reference_id in ({inid})  " +
                  $" order by a.unique_reference_id ";

        }
        public string SelectEpisodedatamigrationjob(string data_migration_job, string entity_type)
        {
            return $"select  entity_id from {data_migration_job} where entity_type='{entity_type}'";
        }
        public string Selectdatamigrationjob(string data_migration_job, string entity_type)
        {
            return $"select * FROM {data_migration_job} where entity_Type='{entity_type}'";
        }
        public string SelectdatamigrationFetchData(string entity_type)
        {
            return $"select entity_type, entity_id, status_id, batch_id  FROM data_migration_job  where entity_Type='{entity_type}'";
        }

        public string ResetDataMigrationStatus(string entityType)
        {
            return $"update Data_Migration_Job t set t.status_id = 1, t.batch_id = null, t.error_details = null, t.external_id = null where t.entity_type = '{entityType}'"; //t.status_id in (2,3,5) and 
        }

        public string InsertSechduleBatchSize(UInt64 batchId, string entityType, int batchSize)
        {
            return $"insert into DATA_MIGRATION_SCHEDULE_BATCH t  (t.id, t.username, t.entity_type, t.batch_size, t.file_generated, t.created_by, t.created_date, t.last_modified_by, " +
                $" t.last_modified_date) values({batchId},'System','{entityType}',{batchSize},'N','System',Sysdate,'System',Sysdate)";
        }

        public string SelectStatusId(UInt64 batchId, string entityType)
        {
            return $"select status_id from data_migration_job where batch_id={batchId} and entity_type='{entityType}' ";
        }


        public string SelectFlag(UInt64 batchId)
        {
            return $"SELECT File_generated FROM data_migration_schedule_batch WHERE id = {batchId}";
        }
        public string SelectScheduleEntryDataByEntityIds(List<string> ids)
        {
            var inid = string.Join(",", ids);

            return $" SELECT 'scheduling' AS entity, " +
                $"  ji.id AS external_id, " +
                $"  TO_CHAR((from_tz(cast(ji.start_date as timestamp), 'Europe/London')), 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM') AS put_up, " +
                $"  CASE " +
                $"  WHEN ji.end_date > TO_DATE('2999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') THEN " +
                $"  TO_CHAR((from_tz(cast(TO_DATE('2999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') as timestamp), 'Europe/London')), 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM') " +
                $"  ELSE " +
                $"  TO_CHAR((from_tz(cast(ji.end_date as timestamp), 'Europe/London')), 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM') " +
                $"  END AS take_down, " +
                $"  'false' AS automatic_asset_selection, " +
                $"  CASE " +
                $"  WHEN pp.id = 97 THEN '666c2393-96c6-4467-8691-c4df72191ea0' " +
                $"  ELSE CASE WHEN pp.id IN(5, 20) THEN 'f2b295fa-ec92-46e9-b204-dccfa8e8fccf' ELSE '-' END " +
                $"  END AS link_platform, " +
                $"  REPLACE(a.unique_reference_id, '/', '-') AS link_title, " +
                $"  F_GET_VARIANT_ID(ml.mm_item_key) AS link_asset, " +
                $"  dmj.linked_entity_ids, " +
                $"  CASE WHEN dm.id = 1 THEN 'Streaming' ELSE dm.description END AS override_distribution_system, " +
                $"  dt.code AS override_resolution, " +
                $"  (select b.code from request_attributes a " +
                $"  join definition_type b on b.id = a.definition_type_id where a.job_item_id in(dmj.linked_entity_ids)) As override_resolution2 " +
                $"  FROM job_item ji " +
                $"  JOIN data_migration_job dmj ON dmj.entity_id = ji.id AND dmj.entity_type IN('Scheduling-Apple', 'Scheduling-Britbox') " +
                $"  JOIN content_plan_item_request cpir ON cpir.job_item_id = ji.id " +
                $"  JOIN content_plan_item cpi ON cpi.id = cpir.content_plan_item_id " +
                $"  JOIN provider_platform_codec ppc ON ppc.id = cpi.provider_platform_codec_id " +
                $"  JOIN provider_platform pp ON ppc.provider_platform_id = pp.id " +
                $"  JOIN asset_metadata am ON am.id = ji.asset_metadata_id " +
                $"  JOIN asset a ON a.id = am.asset_id " +
                $"  JOIN media_lock ml ON ml.job_item_id = ji.id " +
                $"  JOIN delivery_mechanism dm ON dm.id = ppc.delivery_mechanism_id " +
                $"  JOIN request_attributes ra ON ra.job_item_id = ji.id " +
                $"  JOIN definition_type dt ON dt.id = ra.definition_type_id " +
                $"  WHERE ji.id in ({inid}) " +
                $"  order by external_id asc";
        }
        public string SelectRenditionLevelMetadata(string transcodedId)
        {
            string Items = "'" + transcodedId + "'";
            return $" select distinct jitf.transcoded_file_id, jitf.job_item_id,ji.id as id1,ji.Provider_Platform_Id as PROVIDER_PLATFORM_ID1,pp.id,pp.distributor_code,pp.PROVIDER_ID, " +
             $" ppc.PROVIDER_PLATFORM_ID,cp.resolution_width, cp.resolution_height,cp.bit_rate as bit_rate1 , " +
             $" tf.FILE_NAME,tf.FILE_SIZE as filesize1,tf.DURATION as duration1,tf.BIT_RATE,tf.CREATED_DATE,tf.CHECKSUM as CHECKSUM1,tfsl.TRANSCODED_FILE_STATUS,tfsl.CHECKSUM,tfsl.FILESIZE,mi.variant_id,tfsl.ASPECT_RATIO, " +
             $" jitf.job_item_id || '^' || jitf.transcoded_file_id " +
             $" ,tft.SOF, tft.SOM, tft.PRG_DURATION , tft.DURATION " +
             $" from job_item ji " +
             $" join job_item_transcoded_file jitf on jitf.job_item_id = ji.id and jitf.ACTIVE_FLAG='Y' " +
             $" join provider_platform pp on ji.Provider_Platform_Id= pp.id " +
             $" join transcoded_file tf on tf.id = jitf.transcoded_file_id " +
             $"  join transcoded_file_status_log tfsl on tfsl.TRANSCODED_FILE_ID = jitf.transcoded_file_id and tfsl.TRANSCODED_FILE_STATUS ='COMPLETED' " +
             $" join media_lock ml on ml.job_item_id= ji.id " +
                 $" join mm_item mi on mi.mm_item_key= ml.mm_item_key " +
                 $" join provider_platform_codec ppc on ppc.provider_platform_id = pp.id and ppc.active_flag ='Y' and ppc.asset_type_id=2 " +
                 $" join codec_profile_type cp on cp.id = ppc.CODEC_PROFILE_TYPE_ID " +
                 $"left join transcoded_file_timings tft on tft.TRANSCODED_FILE_ID = tf.id " +
                 $" where jitf.transcoded_file_id in ({Items}) and pp.id =22 ";
                
        }
        public string SelectSegmentMetaData(string variantId)
        { 
            string Items = "'" + variantId + "'";
        
            return $"SELECT MT.MM_TIME_MATERIAL_ID,DP.PART_NUMBER, " +
                   $"C2_TIMEUTILS.F_TIME_AS_STRING(DP.START_TIME, 'tfhhmmssff') START_TIME," +
                   $"C2_TIMEUTILS.F_TIME_AS_STRING(DP.END_TIME, 'tfhhmmssff') END_TIME,MI.VARIANT_ID," +
                   $"C2_TIMEUTILS.F_TIME_AS_STRING(dp.duration, 'tfhhmmssff') Duration, " +
                   $"DP.SKIP_INTRO_FLAG INTRO_FLAG, " +
                   $"CASE WHEN DP.SKIP_INTRO_START_TIME > 0 THEN  C2_TIMEUTILS.F_TIME_AS_STRING(DP.SKIP_INTRO_START_TIME, 'tfhhmmssff') " +
                   $"ELSE NULL END AS INTRO_STARTTIME, " +
                   $"CASE WHEN DP.SKIP_INTRO_END_TIME >0 THEN C2_TIMEUTILS.F_TIME_AS_STRING(DP.SKIP_INTRO_END_TIME, 'tfhhmmssff') " +
                   $" ELSE NULL END AS INTRO_ENDTIME, " +
                   $" DP.END_CREDIT_SQUEEZE_FLAG CREDIT_FLAG, " +
                   $" CASE WHEN DP.END_CREDIT_SQUEEZE_START_TIME > 0 THEN C2_TIMEUTILS.F_TIME_AS_STRING(DP.END_CREDIT_SQUEEZE_START_TIME, 'tfhhmmssff') " +
                   $"ELSE NULL END AS CREDIT_STARTTIME, " +
                   $"CASE WHEN DP.END_CREDIT_SQUEEZE_END_TIME > 0 THEN C2_TIMEUTILS.F_TIME_AS_STRING(DP.END_CREDIT_SQUEEZE_END_TIME, 'tfhhmmssff') " +
                   $"ELSE NULL END AS CREDIT_ENDTIME, " +
                   $"CASE WHEN (DP.END_CREDIT_SQUEEZE_START_TIME + DP.END_CREDIT_DUR)> 0 THEN " +
                   $"C2_TIMEUTILS.F_TIME_AS_STRING(DP.end_credit_squeeze_start_time + dp.end_credit_dur, 'tfhhmmssff') " +
                   $"ELSE NULL END AS CR_END,D.Last_Modified_Date " +
                   $"FROM DUBS D " +
                   $"JOIN DUB_PARTS DP ON DP.DUB_ID = D.ID " +
                   $"JOIN MM_TIME MT ON MT.MM_TIME_KEY= DP.MM_TIME_KEY AND MT.MM_ITEM_KEY= D.MM_ITEM_KEY " +
                   $"JOIN MM_ITEM MI ON MI.MM_ITEM_KEY=D.MM_ITEM_KEY " +
                   $"WHERE MI.VARIANT_ID in ({Items}) AND D.STATUS = 1 ORDER BY DP.PART_NUMBER; ";
              
        }
        public string SelectAssetLevelMetadata(string ctrNumber)
        {
            string Items = "'" + ctrNumber + "'";
            return  $" SELECT DISTINCT mi.variant_id, " +
                    $"mi.mm_item_state_nam as mm_item_nam, " +
                    $" mi.mm_item_key, " +
                    $" c.cli_item_nam, " +
                    $"               cli.cli_item_nam itemstatus, " +
                    $"  a.id AS asset_id, " +
                    $" (SELECT c2_timeutils.F_TIME_AS_STRING(SUM(C2_TIMEUTILS.F_CONV_HHMMSSFF_TO_NUMBER(v.MM_TIME_DURATION_COD)), " +
                    $"                 'tfhhmmssff3') " +
                    $"  FROM v_vod_media_details v " +
                    $"  WHERE v.ITEM_KEY = mi.mm_item_key) AS duration," +
                    $"   d.type_of_media,  " +
                    $" case when ma.mm_act_audio_desc_cod='N' or ma.mm_act_audio_desc_cod is null then 'False' else 'True' end  audio_description, " +
                    $" case when ma.MM_ACT_SIGNED_COD='N' or ma.MM_ACT_SIGNED_COD is null then 'False' else 'True' end  signed, " +
                    $"  case when d.type_of_media='L2V' then 2 " +
                    $"   when cli.cli_item_nam='T : Transmission Ready' then 1 " +
                    $"     when cli.cli_item_nam<>'T : Transmission Ready' then 2 end AssetUsage, " +
                    $"  C4_Metadata.c4notes guidance_notes," +
                    $"    Sky_Metadata.SkyNotes short_guidance_notes,  " +
                    $" decode (Sky_Metadata.SkyRating ,null,C4_Metadata.C4Rating,Sky_Metadata.SkyRating) certificate, " +
                    $"   C4_Metadata.approvevoddate," +
                    $"  F_DM_CONV_SECONDS_TO_TIMECODE_ASSET(amest.preview_period_runtime) clip_start_time, " +
                    $"  NVL(ma.mm_act_frames_rate_num, 0) framerate, " +
                    $" F_DM_SUBTITLE_COUNT(mi.variant_id) subtitlecount, " +
                    $"   cAratio.Cli_Item_Cod as Aspect_Ratio, " +
                    $" case when cLines.Cli_Item_Cod <= 625 then 'SD' else 'HD' end As ItemLines," +
                    $"  F_DM_SUBTITLE_VERSION(mi.variant_id) SubTitleVersion " +
                    $" FROM mm_item mi  " +
                    $"   LEFT JOIN asset a " +
                    $" ON a.ctr_contrt_num = to_number(regexp_substr(mi.variant_id, '[0-9]+')) " +
                    $" AND a.prg_prog_num = " +
                    $" to_number(regexp_substr(SUBSTR(mi.variant_id, " +
                    $" INSTR(mi.variant_id, '/')), " +
                    $"   '[0-9]+')) " +
                    $"  AND a.asset_type_id = 2 " +
                    $"      LEFT JOIN ( " +
                    $"    SELECT DISTINCT mi1.variant_id,ml.material_id, " +                  
                    $"  camc4.compliance_notes  as c4Notes, " +
                    $"  rt.short_description as C4Rating, " +
                    $"   (select max(ama.last_modified_date) from asset_metadata_audit ama where ama.asset_id = camc4.asset_id and ama.compliance_completed_flag ='Y') as ApproveVODDate " +
                    $"  FROM media_lock ml  " +
                    $"  JOIN mm_item mi1 ON mi1.mm_item_key = ml.mm_item_key " +
                    $"  left JOIN job_item jiC4 ON jiC4.id = ml.job_item_id " +
                    $"   JOIN asset_metadata camC4  " +
                    $"  ON camC4.id = jiC4.compliance_metadata_id " +
                    $"  AND camc4.asset_metadata_type_id = 2722671 " +
                    $"  JOIN RATING_TYPE rt " +
                    $"  ON rt.ID = camC4.RATING_TYPE_ID " +
                    $"   WHERE camC4.compliance_completed_flag = 'Y') C4_Metadata " +
                    $"   ON C4_Metadata.variant_id = mi.variant_id " +
                    $"   LEFT JOIN ( " +
                    $"  SELECT DISTINCT mi_sky.variant_id,ml_sky.material_id,jtSky.id, " +
                    $"   camsky.compliance_notes  as SkyNotes, " +
                    $"  decode(jtSky.id, 1,rt_sky.short_description,null) as SkyRating " +
                    $"   FROM media_lock ml_sky " +
                    $"  JOIN mm_item mi_sky ON mi_sky.mm_item_key = ml_sky.mm_item_key " +
                    $"  left JOIN job_item ji_sky ON ji_sky.id = ml_sky.job_item_id " +
                    $"  join job j_sky on j_sky.id= ji_sky.job_id " +
                    $" left join job_type jtSky on jtSky.Id= j_sky.job_type_id " +
                    $"  left JOIN asset_metadata camsky " +
                    $"   ON camsky.Id = ji_sky.compliance_metadata_id " +
                    $"   AND camsky.asset_metadata_type_id = 2722677 " +
                    $" JOIN RATING_TYPE rt_sky  " +
                    $" ON rt_sky.ID = camsky.RATING_TYPE_ID  " +
                    $"  WHERE camsky.compliance_completed_flag = 'Y') Sky_Metadata " +
                    $"  ON Sky_Metadata.variant_id = mi.variant_id " +
                    $"   LEFT JOIN ASSET_METADATA amest " +
                    $"   ON amest.asset_id = a.id " +
                    $"  AND amest.editorially_approved_flag = 'Y' " +
                    $" AND amest.asset_metadata_type_id = 2722665  " +
                    $"  JOIN mm_file mf " +
                    $"  ON mf.mm_file_key = mi.mm_file_key " +
                    $" JOIN MM_TIME mt  " +
                    $"    ON mt.MM_ITEM_KEY = mi.mm_item_key " +
                    $"  JOIN MM_ACT ma  " +
                    $"  ON ma.mm_act_key = mt.mm_act_key " +
                    $"    JOIN dls_media_loc_and_type d " +
                    $"   ON d.mm_file_id_cod = mf.mm_file_id_cod " +
                    $"  LEFT JOIN vers v " +
                    $"  ON v.vers_key = mi.vers_key " +
                    $" LEFT JOIN cli c  " +
                    $" ON c.cli_key = v.vers_progman_cli_key  " +
                    $"  JOIN CLI cli " +
                    $"  ON mi.mm_item_status_cli_key = cli.cli_key " +
                    $"  JOIN cli cli2 " +
                    $"  ON cli2.cli_key = ma.mm_act_type_cli_key " +
                    $"   JOIN cli cAratio " +
                    $"  ON cAratio.Cli_Key = ma.mm_act_afd_ratio_cli_key " +
                    $"  JOIN cli cLines " +
                    $"  ON cLines.Cli_Key = ma.mm_act_lines_cli_key " +
                    $"  WHERE d.type_of_media IN ('MSPDAM', 'L2V') " +
                    $"  AND d.active_flag = 'Y' " +
                    $"  AND cli2.cli_title_cod = 'MMACT' " +
                    $" AND cli2.cli_item_desc_tex IN ('Reviewed',  " +
                    $"   'Safety Copy', " +
                    $"  'Archive Dub & Review', " +
                    $"   'Simultaneous Define & Review', " +
                    $"    'Dubbing Copy') " +
                    $"   AND mt.mm_time_material_id IS NOT NULL " +
                    $"     AND mf.mm_file_id_cod IN ('PFTM1', 'M2AMEDIA') " +
                    $"  AND mi.mm_item_present_flg = 'Y' and mi.variant_id in ( {Items})  ";                                 
                    

        }
        public string SelectApprovedValue(string variantId)
        {
            string Items = "'" + variantId + "'";

            return $" select mi1.variant_id,camc4.id,camc4.asset_id, ml.material_id ,camc4.compliance_notes as Notes ,rt.short_description as Rating " +
                   $" ,(select max(ama.last_modified_date) from asset_metadata_audit ama where ama.asset_id = camc4.asset_id and ama.compliance_completed_flag ='Y') as Approve_VODDate " +
                   $" from media_lock ml " +
                   $" join mm_item mi1 on mi1.mm_item_key = ml.mm_item_key " +
                   $" join job_item ji on ji.id = ml.job_item_id " +
                   $" join asset_metadata camC4 on camC4.id= ji.compliance_metadata_id " +
                   $" JOIN RATING_TYPE rt ON rt.ID = camC4.RATING_TYPE_ID" +
                   $" join provider_platform pp on pp.id = ji.provider_platform_id " +
                   $" where pp.approval_share_group_id =95 " +
                   $" and camC4.compliance_completed_flag = 'Y' " +
                   $" and camc4.compliance_notes is not null " +
                   $" and camc4.asset_metadata_type_id = 2722671 " +
                   $" and mi1.variant_id in  ({Items}) " +                   
                   $" fetch first 1 row only ";
        }

        public string SelectAssetMaterialDataByEntityIds(List<string> ids)
        {
            var inid = string.Join(",", ids);
            var arrayItems = inid.Split(",");
            string Items = "";

            for (int item = 0; item <= arrayItems.Length - 1; item++)
            {
                Items = Items + "'" + arrayItems[item] + "'";
                if (item != arrayItems.Length - 1)
                {
                    Items += ",";
                }
            }

            return $" WITH cte_asset as (" +
                $"  SELECT mi.variant_id, a.id, a.ctr_contrt_num, a.prg_prog_num " +
                $"  FROM media_lock ml " +
                $"  JOIN mm_item mi ON mi.mm_item_key = ml.mm_item_key " +
                $"  JOIN job_item ji ON ji.id = ml.job_item_id " +
                $"  JOIN asset_metadata am ON am.id = ji.asset_metadata_id " +
                $"  JOIN asset a ON a.id = am.asset_id " +
                $"  WHERE ji.Provider_Platform_id IN(5, 20, 97) " +
                $"  AND mi.variant_id IS NOT NULL ) " +
                $"  SELECT DISTINCT  mi.variant_id AS external_id, " +
                $"  mi.variant_id AS asset_name, " +
                $"  mi.mm_item_nam AS asset_description, " +
                $"  c.cli_item_nam AS asset_type," +
                $"  a.id AS asset_id, " +
                $"  (a.ctr_contrt_num || '-' || TO_CHAR(a.prg_prog_num, 'fm000')) as asset_title, " +
                $"  (SELECT c2_timeutils.F_TIME_AS_STRING(SUM(C2_TIMEUTILS.F_CONV_HHMMSSFF_TO_NUMBER(v.MM_TIME_DURATION_COD)), 'tfhhmmssff3') " +
                $"   FROM v_vod_media_details v " +
                $"   WHERE v.ITEM_KEY = mi.mm_item_key) AS duration " +
                $"   FROM mm_item mi " +
                $"   JOIN cte_asset a ON a.variant_id = mi.variant_id " +
                $"   JOIN mm_file mf ON mf.mm_file_key = mi.mm_file_key " +
                $"   JOIN dls_media_loc_and_type d ON d.mm_file_id_cod = mf.mm_file_id_cod " +
                $"   LEFT JOIN vers v on v.vers_key = mi.vers_key " +
                $"   LEFT JOIN cli c on c.cli_key = v.vers_progman_cli_key " +
                $"   JOIN cli cli1 ON cli1.cli_key = mi.mm_item_status_cli_key " +
                $"   WHERE mi.mm_item_status_cli_key = 267552656 " +
                $"   AND d.type_of_media = 'MSPDAM' " +
                $"   AND mf.mm_file_id_cod = 'PFTM1' " +
                $"   AND mi.mm_item_present_flg = 'Y' " +
                $"  AND mi.variant_id in ({Items}) order by external_id asc ";
        }

        public string SelectEpisodeMetaData(List<string> ids)
        {
            var inid = string.Join(",", ids);
            var arrayItems = inid.Split(",");
            string Items = "";

            for (int item = 0; item <= arrayItems.Length - 1; item++)
            {
                Items = Items + "'" + arrayItems[item] + "'";
                if (item != arrayItems.Length - 1)
                {
                    Items += ",";
                }
            }
            return $" select c4.title,replace( a.unique_reference_id,'/','-') external_id,a.description Name,a.prg_prog_num EpisodeNumber, " +
                $"  a.ctr_contrt_num,c4.sequence_number Running_order,c4.summary_medium Short_Synopsys,c4.description Long_Synopsys, " +
                $"  v.description Extra_short_synpsys,c4.significant_date first_tx_date, " +
                $"  'Episode in Series' ContentType  from Asset a " +
                $"  left join flat_asset_metadata c4 on a.id=c4.asset_id and c4.asset_metadata_type_id=2722671 and c4.editorially_approved_flag='Y' " +
                $"  left join flat_asset_metadata v on a.id=v.asset_id and v.asset_metadata_type_id=2722669 and v.editorially_approved_flag='Y' " +
                $"  where a.unique_reference_id in({Items}) ";


        }
        public string SelectOneEpisodeMetaData(string id)
        {
            string Items = "'" + id + "'";

            return $" select c4.title,replace( a.unique_reference_id,'/','-') external_id,a.description Name,a.prg_prog_num EpisodeNumber, " +
                $"  a.ctr_contrt_num,c4.sequence_number Running_order,c4.summary_medium Short_Synopsys,c4.description Long_Synopsys, " +
                $"  v.description Extra_short_synpsys,c4.significant_date first_tx_date, " +
                $"  'Episode in Series' ContentType  from Asset a " +
                $"  left join flat_asset_metadata c4 on a.id=c4.asset_id and c4.asset_metadata_type_id=2722671 and c4.editorially_approved_flag='Y' " +
                $"  left join flat_asset_metadata v on a.id=v.asset_id and v.asset_metadata_type_id=2722669 and v.editorially_approved_flag='Y' " +
                $"  where a.unique_reference_id in({Items}) ";


        }
        public string SelectOneFeatureMetaData(string id)
        {
            string Items = "'" + id + "'";

            return $" select  f.asset_id, a.description Name, ep.prg_prog_num EpisodeNumber,b.id BrandId, p.sequence_number RunningOrder, " +
                $"  ep.movie_flag,b.id BrandID,c.brand_family, c.genre_enum_code Genre,c.collection strand, " +
                $"  f.significant_date Fx_date, " +
                $"  f.description ShorSynopsys, v.description ExtreShortSynopsys, p.name YoutubeMatchPolicy  from Asset a " +
                $"  left join  flat_asset_metadata f  on a.id=f.asset_id  and f.asset_metadata_type_id=2722671 " +
                $"  left join  flat_asset_metadata v  on a.id=v.asset_id  and v.asset_metadata_type_id=2722669 " +
                $"  left join  flat_asset_metadata yt  on a.id=yt.asset_id  and yt.asset_metadata_type_id=2722675 " +
                $"  left join policy  p on p.id=yt.policy_id " +
                $"  left join asset_parent p on p.asset_parent_id=a.id " +
                $"  left join asset b on b.id=p.asset_parent_id and b.asset_type_id=3 " +
                $"  left join data_migration_content_data c on c.brand_id=b.id " +
                $"  left join asset ep on ep.ctr_contrt_num=a.ctr_contrt_num and ep.asset_type_id=2 " +
                $"  left join data_migration_content_data c on c.brand_id=b.id " +
                $"  where a.ctr_contrt_num in ({Items}) and a.asset_type_id=1; ";
        }

        public string SelectMovieFlagFeatureMetaData (string ctrNum)
        {
            string Items = "'" + ctrNum + "'";
            return $"select se.ctr_contrt_num,se.movie_flag, " +
                   $"(SELECT COUNT(*) " +
                   $" FROM app_cgs.ASSET EPA " +
                   $"JOIN app_cgs.ASSET_PARENT AP " +
                   $"ON EPA.ID = AP.ASSET_ID " +
                   $" where AP.ASSET_PARENT_ID = SE.ID) TOTAL_EPISDOES_COUNT " +
                   $" from  app_cgs.asset se where se.ctr_contrt_num in ({Items}) and se.asset_type_id=1";
        }
        public string SelectCountOfMovieFlagFeatureMetData(string ctrNum, string movieFlag)
        {
            string Items = "'" + ctrNum + "'";
            return $"select se.ctr_contrt_num,se.movie_flag, " +
                   $"(SELECT COUNT(*) " +
                   $" FROM app_cgs.ASSET EPA " +
                   $"JOIN app_cgs.ASSET_PARENT AP " +
                   $"ON EPA.ID = AP.ASSET_ID " +
                   $" where AP.ASSET_PARENT_ID = SE.ID) TOTAL_EPISDOES_COUNT " +
                   $" from  app_cgs.asset se where se.ctr_contrt_num in ({Items}) and se.asset_type_id=1";
        }
        public string SelectRightsMetaDataView(List<string> ids)
        {
            var inid = string.Join(",", ids);
            return $"select * from v_dm_vod_archive_rights in ({inid}) ";
        }
        public string SelectRightsMetaDataName(List<string> ids)
        {
            var inid = string.Join(",", ids);
            return $"SELECT ZZ.ctr_contrt_num||'/'||ZZ.agr_generic_ref_cod ||'/'||'TAG-'||ZZ.RNUM||'/'|| ZZ.name as \"Term_Name\",ZZ.TERM_ID FROM ( " +
                   $"select  c.ctr_contrt_num,ag.agr_generic_ref_cod,ral.finance_agr_line_id,ral.id,ral.asset_group_id, " +
                   $"DENSE_RANK() OVER(PARTITION BY ral.finance_agr_line_id order by ral.ID ) RNUM, " +
                   $"rt.name,rt.start_date,rt.end_date, RT.ID AS TERM_ID" +
                   $"from rights_terms rt " +
                   $"left join rights_agreement_lines ral on rt.rights_agreement_line_id = ral.id " +
                   $"left join app_pirate.ait a on ral.finance_agr_line_id = a.ait_key " +
                   $"left join ctr c on a.ctr_key = c.ctr_key " +
                   $"left join agr ag on ag.prjdet_projct_num =a.prjdet_projct_num and a.agr_agrmnt_num= ag.agr_agrmnt_num  ) ZZ " +
                   $"WHERE ZZ.TERM_ID in ({inid})" +
                   $"order by  ZZ.TERM_ID asc";
        }
        //        public string GetContrNumFArchive(List<string> ids)
        //        {
        //            var inid = string.Join(",", ids);
        //            return $"select  c.ctr_contrt_num,c.ctr_key,ag.agr_generic_ref_cod, " +
        //                   $"c.ctr_contrt_num || '/' || ag.agr_generic_ref_cod || '/' || rt.name as \"Term_Name\", "+
        //                    "rt.id as rights_term_id,ral.id, rt.entity_type_id as VOD_rights,rt.name as right_name " +
        //                    "from rights_terms rt"+
        //                    "left"=
        //join rights_agreement_lines ral on rt.rights_agreement_line_id = ral.id
        //left
        //join app_pirate.ait a on ral.finance_agr_line_id = a.ait_key
        //left
        //join ctr c on a.ctr_key = c.ctr_key
        //left
        //join agr ag on ag.prjdet_projct_num = a.prjdet_projct_num and a.agr_agrmnt_num = ag.agr_agrmnt_num
        //where rt.id";
        //        }

    }
}
