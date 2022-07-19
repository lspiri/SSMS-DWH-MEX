use MX_DWH
Go

if object_id ('etl.map_lk_servicio_concepto', 'P') is not null
	drop procedure etl.map_lk_servicio_concepto
GO

create procedure etl.map_lk_servicio_concepto (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 28/03/2022
	-- Description:	Mapping LK Pri Servicio
	-- =============================================
	-- 28/03/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Bajada
		select codigo as codigo, 
			etl.fn_trim(descripcion) as descripcion,
			id_tipo_serv_detalle
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion,
				etl.fn_LookupIdTipoServDetalle(null) as id_tipo_serv_detalle
			union all
			select 1 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion,
				etl.fn_LookupIdTipoServDetalle(null) as id_tipo_serv_detalle
			union all
			select distinct 2 as orden,
				st.cod_cuenta, 
				st.desc_cuenta,
				etl.fn_LookupIdTipoServDetalle(null) as id_tipo_serv_detalle
			from stg.ST_DGEN_LK_CTA_CONTABLE st
		) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc, id_tipo_serv_detalle asc)

		begin transaction
				
		-- Insertar Nuevo
		insert into dbo.DFAC_LK_SERVICIO_CONCEPTO (
			cod_serv_concepto, desc_serv_concepto, id_tipo_serv_detalle)
		select codigo, descripcion, id_tipo_serv_detalle
		from #datos a
		where not exists (select 1 
			from dbo.DFAC_LK_SERVICIO_CONCEPTO b
			where b.cod_serv_concepto = a.codigo
				and b.id_tipo_serv_detalle = a.id_tipo_serv_detalle)

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK  
		set desc_serv_concepto = b.descripcion
		from dbo.DFAC_LK_SERVICIO_CONCEPTO LK, #datos b
		where b.codigo = LK.cod_serv_concepto
			and b.id_tipo_serv_detalle = LK.id_tipo_serv_detalle
			and (b.descripcion <> LK.desc_serv_concepto)

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
-- exec etl.map_lk_servicio_concepto 0