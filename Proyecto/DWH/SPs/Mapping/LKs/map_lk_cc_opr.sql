use MX_DWH
Go

if object_id ('etl.map_lk_cc_opr', 'P') is not null
	drop procedure etl.map_lk_cc_opr
GO

create procedure etl.map_lk_cc_opr (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31/03/2022
	-- Description:	Mapping LK CC OPR
	-- =============================================
	-- 04/04/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500)

		-- Inicio
		set @msg = '>>- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		-- Bajada
		select --id_cc, 
		    etl.fn_trim(cod_cc) as cod_cc,
			etl.fn_trim(desc_cc) as desc_cc,
			etl.fn_trim(region_opr) as region_opr,
			etl.fn_trim(dop_opr) as dop_opr,
			etl.fn_trim(region_opr_2) as region_opr_2,
			id_agrupador
		into #datos
		from (
			select 1 as orden,
				--'-1' as id_cc,
				'-1' as cod_cc, 
				'Not Exists' as desc_cc,
				'NA' as region_opr,
				'NA' as dop_opr,
				'NA' as region_opr_2,
				etl.fn_LookupIdAgrupadorCC('NA') as id_agrupador
			union all
			select 1 as orden,
				 --0 as id_cc,
				'NA' as cod_cc, 
				'Not Available' as desc_cc,
				'NA' as region_opr,
				'NA' as dop_opr,
				'NA' as region_opr_2,
				etl.fn_LookupIdAgrupadorCC('NA') as id_agrupador
			union all
			select distinct 2 as orden,  
				isnull(cod_cc,'') cod_cc, 
				isnull(desc_cc,'') desc_cc,
				isnull(region_opr,'') region_opr,
				isnull(dop_opr,'') dop_opr,
				isnull(region_opr_2, isnull(region_opr,'')) as region_opr_2,
				etl.fn_LookupIdAgrupadorCC(agrupador) as id_agrupador
			from stg.ST_DGEN_LK_CC_OPR a
			where cod_cc is not null) x
		order by x.orden, x.desc_cc

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (cod_cc asc)

		-- Insertar Nuevo
		insert into dbo.DGEN_LK_CC_OPR (
			 cod_cc, desc_cc, region_opr, dop_opr, region_opr_2, id_agrupador)
		select cod_cc, desc_cc, region_opr, dop_opr, region_opr_2, id_agrupador
		from #datos a
		where not exists (select 1 
			from dbo.DGEN_LK_CC_OPR b
			where b.cod_cc = a.cod_cc)

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK  
		set desc_cc = b.desc_cc,
			region_opr =  b.region_opr,
			region_opr_2 =  b.region_opr_2,
			dop_opr = b.dop_opr,
			id_agrupador = b.id_agrupador
		from dbo.DGEN_LK_CC_OPR LK, #datos b
		where b.cod_cc = LK.cod_cc
			and( (isnull(b.desc_cc,'-99') <> isnull(LK.desc_cc,'-99'))
			or (isnull(b.region_opr,'-99') <> isnull(LK.region_opr,'-99')) 
			or (isnull(b.region_opr_2,'-99') <> isnull(LK.region_opr_2,'-99')) 
			or (isnull(b.dop_opr,'-99') <> isnull(LK.dop_opr,'-99')) 
			or b.id_agrupador <> LK.id_agrupador )


		set @msg = 'Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		commit transaction

	end try
	
	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch
	end catch
end
GO
-- exec etl.map_lk_cc_opr 0