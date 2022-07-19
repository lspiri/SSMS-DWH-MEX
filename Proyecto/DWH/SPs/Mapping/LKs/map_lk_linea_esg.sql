use MX_DWH
Go

if object_id ('etl.map_lk_linea_esg', 'P') is not null
	drop procedure etl.map_lk_linea_esg
GO

create procedure etl.map_lk_linea_esg (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31/03/2022
	-- Description:	Mapping LK Linea Estado Gestion
	-- =============================================
	-- 16/06/2022 - Version Inicial


	set nocount on

	begin try
		declare @msg varchar(500)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		
		-- Bajada
		select codigo as codigo, 
			etl.fn_trim(descripcion) as descripcion,
			desc_linea_n2,
			desc_linea_n3,
			id_linea2,
			id_linea3,
			cod_cuenta,
			cod_servicio,
			calculo,
			flag
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion,
				'Not Exists' as desc_linea_n2,
				'Not Exists' as desc_linea_n3,
				-1 as id_linea2,
				-1 as id_linea3,
				'-1' as cod_cuenta,
				'-1' as cod_servicio,
				null as calculo,
				0 as flag
			union all
			select 0 as orden,
				'0' as codigo, 
				'Not Available' as descripcion,
				'Not Available' as desc_linea_n2,
				'Not Available' as desc_linea_n3,
				0 as id_linea2,
				0 as id_linea3,
				'NA' as cod_cuenta,
				'NA' as cod_servicio,
				null as calculo,
				0 as flag
			union all
			select 1 as orden, max(cod_linea) as cod_linea, 
				max(isnull(desc_linea,'')) as desc_linea,
				max(isnull(desc_linea_n2,'')) as desc_linea_n2,
				max(isnull(desc_linea_n3,'')) as desc_linea_n3,
				max(isnull(id_linea2,0)) as id_linea2,
				max(id_linea3) as id_linea3,
				isnull(cod_cuenta,'NA') as cod_cuenta,
				isnull(cod_servicio,'NA') as cod_servicio,
				max(calculo) as calculo,
				'1' as flag
			from stg.ST_DESG_LK_LINEA
			where cod_linea is not null
			group by isnull(cod_cuenta,'NA'), isnull(cod_servicio,'NA') ) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (cod_cuenta asc, cod_servicio asc)

		begin transaction

		-- Insertar Nuevo
		insert into dbo.DESG_LK_LINEA (
			cod_linea, desc_linea, desc_linea_n2, desc_linea_n3,
			id_linea2, id_linea3, cod_cuenta, cod_servicio, calculo, flag)
		select codigo, descripcion, desc_linea_n2, desc_linea_n3,
			id_linea2, id_linea3, cod_cuenta, cod_servicio, calculo, flag
		from #datos a
		where not exists (select 1 
			from dbo.DESG_LK_LINEA b
			where b.cod_cuenta = a.cod_cuenta
				and b.cod_servicio = a.cod_servicio)
		order by a.codigo

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update lk
		set desc_linea = b.descripcion,
			desc_linea_n2 = b.desc_linea_n2,
			desc_linea_n3 = b.desc_linea_n3,
			id_linea2 = b.id_linea2, 
			id_linea3 = b.id_linea3, 
			--cod_cuenta = b.cod_cuenta, 
			--cod_servicio = b.cod_servicio,
			cod_linea = b.codigo,
			calculo = b.calculo,
			flag = b.flag
		from dbo.DESG_LK_LINEA lk, 
			#datos b
		where b.cod_cuenta = lk.cod_cuenta
			and b.cod_servicio = lk.cod_servicio
			and( isnull(b.descripcion,'-99') <> isnull(lk.desc_linea,'-99')
				or isnull(b.desc_linea_n2,'-99') <> isnull(lk.desc_linea_n2,'-99')
				or isnull(b.desc_linea_n3,'-99') <> isnull(lk.desc_linea_n3,'-99')
				or isnull(b.id_linea2,-99) <> isnull(lk.id_linea2,-99)
				or isnull(b.id_linea3,-99) <> isnull(lk.id_linea3,-99)
				--or isnull(b.cod_cuenta,'-99') <> isnull(lk.cod_cuenta,'-99')
				--or isnull(b.cod_servicio,'-99') <> isnull(lk.cod_servicio,'-99')
				or b.codigo <> lk.cod_linea
				or isnull(b.flag,'9') <> isnull(lk.flag,'9')
				or isnull(b.calculo,'-99') <> isnull(lk.calculo,'-99'))

		set @msg = 'Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update lk
		set flag = 0
		from dbo.DESG_LK_LINEA lk 
		where not exists (select 1 from #datos b
			where b.cod_cuenta = lk.cod_cuenta
				and b.cod_servicio = lk.cod_servicio)

		set @msg = 'Descartados (Flag): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.map_lk_linea_esg 0