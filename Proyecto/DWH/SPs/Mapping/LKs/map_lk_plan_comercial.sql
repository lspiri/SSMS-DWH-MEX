use MX_DWH
Go

if object_id ('etl.map_lk_plan_comercial', 'P') is not null
	drop procedure etl.map_lk_plan_comercial
GO

create procedure etl.map_lk_plan_comercial (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 28-04-2022
	-- Description:	Mapping LK Plan Comercial
	-- =============================================
	-- 28/04/2022 - Version Inicial


	set nocount on;
	begin try

		declare @msg varchar(500)
		
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		-- Bajada
		select etl.fn_trim(codigo) as cod_plan_comercial_opp, 
			etl.fn_trim(descripcion) as desc_plan_comercial_opp
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion
			union all
			select 0 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion
			union all
			select 2 as orden, cod_plan_comercial_opp, 
				max(isnull(desc_plan_comercial_opp,'')) as desc_plan_comercial_opp
			from stg.ST_DCOT_LK_PLAN_COMERCIAL
			where cod_plan_comercial_opp is not null
			group by cod_plan_comercial_opp) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (cod_plan_comercial_opp asc)

		-- Insertar Nuevo
		insert into dbo.DCOT_LK_PLAN_COMERCIAL (
			cod_plan_comercial_opp, desc_plan_comercial_opp)
		select cod_plan_comercial_opp, desc_plan_comercial_opp
		from #datos a
		where not exists (select 1 
			from dbo.DCOT_LK_PLAN_COMERCIAL b
			where b.cod_plan_comercial_opp = a.cod_plan_comercial_opp)
		
		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK 
		set desc_plan_comercial_opp = b.desc_plan_comercial_opp
		-- select *
		from #datos b,
			dbo.DCOT_LK_PLAN_COMERCIAL LK
		where b.cod_plan_comercial_opp = LK.cod_plan_comercial_opp
		 and (b.desc_plan_comercial_opp <> LK.desc_plan_comercial_opp)
		
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
-- exec etl.map_lk_plan_comercial 0