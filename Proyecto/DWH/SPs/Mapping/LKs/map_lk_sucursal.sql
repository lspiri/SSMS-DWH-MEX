use MX_DWH
Go

if object_id ('etl.map_lk_sucursal', 'P') is not null
	drop procedure etl.map_lk_sucursal
GO

create procedure etl.map_lk_sucursal (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK Sucursal
	-- =============================================
	-- 05/04/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @cant numeric(10), @novedad numeric(10)
			
		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @novedad = 0

		begin transaction

		-- Agrega Agrupadores
		insert into dbo.DGEN_LK_AGRUPADOR_CC  (cod_agrupador, desc_agrupador)
		select distinct agrupador, 
			case when agrupador = cod_sucursal then 
				desc_sucursal else agrupador 
			end as desc_agrupador
		from stg.ST_DGEO_LK_SUCURSAL st
		where cod_sucursal is not null
			and agrupador is not null
			and not exists (select 1 from dbo.DGEN_LK_AGRUPADOR_CC lk
				where lk.cod_agrupador = st.agrupador)

		set @msg = 'Insert Agrupadores (LK): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update lk
		set desc_agrupador = cod_agrupador
		-- select *
		from dbo.DGEN_LK_AGRUPADOR_CC lk
		where (cod_agrupador != desc_agrupador and desc_agrupador = '')

		set @msg = 'Update Agrupadores (LK): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		-- Bajada
		select etl.fn_trim(codigo) as codigo, 
			etl.fn_trim(descripcion) as descripcion,
			id_region,
			marca_ms,
			fecha_baja,
			consultor_bys,
			id_zona,
			id_agrupador
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion,
				etl.fn_LookupIdRegion('NA') as id_region,
				'N' as marca_ms,
				cast(null as datetime) as fecha_baja,
				null as consultor_bys,
				etl.fn_LookupIdZona('NA') as id_zona,
				etl.fn_LookupIdAgrupadorCC('NA') as id_agrupador
			union all
			select 1 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion,
				etl.fn_LookupIdRegion('NA') as id_region,
				'N' as marca_ms,
				cast(null as datetime) as fecha_baja,
				null as consultor_bys,
				etl.fn_LookupIdZona('NA') as id_zona,
				etl.fn_LookupIdAgrupadorCC('NA') as id_agrupador
			union all
			select 2 as orden, cod_sucursal, 
				max(isnull(desc_sucursal,'')),
				etl.fn_LookupIdRegion(max(cod_region)) as id_region,
				'N' as marca_ms,
				max(fecha_baja),
				max(consultor_bys),
				etl.fn_LookupIdZona(max(etl.fn_trim(cod_zona))) as id_zona,
				etl.fn_LookupIdAgrupadorCC(max(agrupador)) as id_agrupador
			from stg.ST_DGEO_LK_SUCURSAL
			where cod_sucursal is not null
			group by cod_sucursal) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		
		
		


		
		-- Insertar Nuevo
		insert into dbo.DGEO_LK_SUCURSAL (
			cod_sucursal, desc_sucursal, marca_ms, id_region, fecha_baja,
			id_zona, id_agrupador)
		select a.codigo, a.descripcion,  a.marca_ms, a.id_region, a.fecha_baja,
			id_zona, id_agrupador
		from #datos a
		where not exists (select 1 
			from dbo.DGEO_LK_sucursal b
			where b.cod_sucursal = a.codigo)

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Insertados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg
		
		-- Actualizar Info
		update LK 
		set desc_sucursal = a.descripcion,
			id_region = a.id_region,
			id_zona = a.id_zona,
			id_agrupador = a.id_agrupador
		from dbo.DGEO_LK_SUCURSAL LK,
			#datos a
		where a.codigo = LK.cod_sucursal
			and (a.descripcion <> LK.desc_sucursal
				or a.id_region <> LK.id_region
				or isnull(a.id_zona,0) <> isnull(LK.id_zona,0)
				or isnull(a.id_agrupador,0) <> isnull(LK.id_agrupador,0))

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
-- exec etl.map_lk_sucursal 0