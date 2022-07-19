use MX_DWH
Go

if object_id ('etl.map_lk_region', 'P') is not null
	drop procedure etl.map_lk_region
GO

create procedure etl.map_lk_region (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK Region
	-- =============================================
	-- 21/03/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @cant numeric(10), @novedad numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @novedad = 0

		begin transaction

		-- Bajada
		select etl.fn_trim(codigo) as codigo, 
			etl.fn_trim(descripcion) as descripcion,
			id_dop, responsable, dr_comite, fte
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion,
				 etl.fn_LookupIdDOP(null) as id_dop,
				 null responsable, null dr_comite,
				 cast(null as numeric) fte
			union all
			select 1 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion,
				etl.fn_LookupIdDOP(null) as id_dop,
				null responsable, null dr_comite,
				cast(null as numeric) fte
			union all
			select 2 as orden, cod_region, 
				max(replace (isnull(desc_region,''), 'region', '')),
				etl.fn_LookupIdDOP(max(cod_dop)),
				max(responsable), max(dr_comite),
				max(fte)
			from stg.ST_DGEO_LK_REGION
			where cod_region is not null
			group by cod_region) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Acomodar Datos
		update #datos
		set descripcion = replace (descripcion, 'PUNTO DE VENTA', 'PDV')
		where descripcion like 'DR ADECCO PUNTO DE VENTA%'
		
		-- Insertar Nuevo
		insert into dbo.DGEO_LK_REGION (
			cod_region, desc_region, id_dop, responsable, dr_comite, fte)
		select codigo, descripcion, id_dop, responsable, dr_comite, fte
		from #datos a
		where not exists (select 1 
			from dbo.DGEO_LK_region b
			where b.cod_region = a.codigo)

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Insertados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK
		set desc_region = b.descripcion,
			id_dop = b.id_dop,
			responsable = b.responsable, 
			dr_comite = b.dr_comite,
			fte = b.fte
		-- select *
		from #datos b, dbo.DGEO_LK_REGION LK
		where b.codigo = LK.cod_region
			and (b.descripcion <> LK.desc_region
				or b.id_dop <> LK.id_dop
				or isnull(b.responsable,'-1') <> isnull(LK.responsable,'-1')
				or isnull(b.dr_comite, '-1') <> isnull(LK.dr_comite,'-1') 
				or isnull(b.fte, -1) <> isnull(LK.fte,-1) )

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Actualizados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		commit transaction

		if (@novedad) = 0 
			return 0
		else
			return 1

	end try
	
	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

		return -1

	end catch
end
GO
-- exec etl.map_lk_region 0