use MX_DWH
Go

if object_id ('etl.map_ft_facturacion_serv_concep', 'P') is not null
	drop procedure etl.map_ft_facturacion_serv_concep
GO

create procedure etl.map_ft_facturacion_serv_concep (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 28/03/2022
	-- Description:	Mapping MAP FT Servicio Concepto
	-- =============================================
	-- 11/05/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@idMes numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		select @cant = count(1) 
		from stg.ST_FT_FACTURACION_TS
		where tipo_operacion = 'VTA'

		if  @cant = 0 begin
			set @msg = 'El Proceso de Carga no continúa debido a que no existen datos en la fuente.'
			set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
			exec etl.prc_Logging @idBatch, @msgLog
			exec etl.prc_reg_ctrl_mapping @msg, @idBatch
			return 0
		end

		-------------------------------------------
		-- PREPROCESAMIENTO DATOS (PD)
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'PreProcesamiento de Datos:'

		begin transaction

		delete from stg.PD_FT_FACTURACION_SERV_CONCEP
		where id_mes in (select distinct periodo from stg.ST_FT_FACTURACION_TS)

		set @msg = 'Borrados PD: ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

		declare lc_cursor cursor for
		select distinct cast(periodo as numeric) as id_mes
		from stg.ST_FT_FACTURACION_TS
		where tipo_operacion = 'VTA'
		order by 1

		open lc_cursor
		fetch next from lc_cursor into @idMes
		while @@fetch_status = 0
			begin

				set @msg = '- Mes: ' + cast (@idMes as varchar); exec etl.prc_Logging @idBatch, @msg

				insert into stg.PD_FT_FACTURACION_SERV_CONCEP (
					grupo, id_empresa, id_sucursal, id_condic_subclte, id_mes,
					id_subcliente, id_cliente, id_tipo_serv_detalle,
					id_serv_concepto, id_usuario, importe_fact, mb_monto, mb_monto_scf,
					ft_fact, ft_mb, ft_mb_monto_scf, origen)
				select 0 as grupo, id_empresa, id_sucursal, id_condic_subclte, id_mes, 
					id_subcliente, id_cliente, id_tipo_serv_detalle, 
					id_serv_concepto, max(id_usuario), 
					sum(facturacion) as importe_fact, 
					(Sum(facturacion)-Sum(costo)) as mb_monto,
					0 as mb_monto_scf,
					0 as ft_fact,
					0 as ft_mb,
					0 as ft_mb_monto_scf,
					'D365' as origen
				from (
					select id_stage,
						cast(a.periodo as numeric) as id_mes, 
						etl.fn_LookupIdSucursal(a.cod_sucursal) as id_sucursal,
						etl.fn_LookupIdCondicionCte( null) as id_condic_subclte,
						isnull(b.id_cliente,-1) as id_cliente,
						etl.fn_LookupIdTipoServDetalle(a.cod_servicio) as id_tipo_serv_detalle,
						etl.fn_LookupIdSubcliente(null, null) as id_subcliente,
						etl.fn_LookupIdEmpresa(a.cod_empresa) as id_empresa,
						isnull(sc.id_serv_concepto,-1) as id_serv_concepto,
						case when a.tipo_operacion = 'VTA' then isnull(a.importe,0)*-1 else 0 end as facturacion,
						case when a.tipo_operacion = 'COS' then isnull(a.importe,0) else 0 end as costo,
						0 mb_monto,
						0 mb_porc,
						etl.fn_LookupIdUsuario(null) as id_usuario,
						cast(null as numeric) as id_oportunidad,
						etl.fn_LookupIdContacto(null) as id_contacto,
						0 as margen_bruto_teo,  
						0 as facturacion_cotiz,
						etl.fn_LookupIdServicioPri (null) as id_servicio_pri
					from stg.ST_FT_FACTURACION_TS a
						left outer join dbo.DCTE_LK_CLIENTE b 
							on b.cod_cliente = a.cod_cliente
						left outer join DFAC_LK_SERVICIO_CONCEPTO sc
							on sc.cod_serv_concepto = a.cod_cuenta
							 and sc.id_tipo_serv_detalle = 0
					where a.periodo = @idMes
						and a.tipo_operacion in ('VTA', 'COS')
					) vw
				group by id_mes, id_sucursal, id_condic_subclte, id_cliente, 
						id_tipo_serv_detalle, id_subcliente, id_empresa, id_serv_concepto

				set @msg = 'Insertados (D365): ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

				insert into stg.PD_FT_FACTURACION_SERV_CONCEP (
					grupo, id_empresa, id_sucursal, id_condic_subclte, id_mes,
					id_subcliente, id_cliente, id_tipo_serv_detalle,
					id_serv_concepto, id_usuario, importe_fact, mb_monto, mb_monto_scf,
					ft_fact, ft_mb, ft_mb_monto_scf, origen)
				select 0 as grupo, vw.id_empresa, vw.id_sucursal, vw.id_condic_subclte, vw.id_mes, 
					0 as id_subcliente, vw.id_cliente, vw.id_tipo_serv_detalle,  
					vw.id_serv_concepto,
					max(id_usuario) as id_usuario, 
					sum(facturacion) as importe_fact, 
					(Sum(facturacion)-Sum(costo)) as mb_monto,
					0 as mb_monto_scf,
					0 as ft_fact,
					0 as ft_mb,
					0 as ft_mb_monto_scf,
					vw.origen
				from (
					select cast(ods.periodo as numeric) as id_mes,
						ods.tipo as origen,
						etl.fn_LookupIdSucursal(ods.cod_sucursal) as id_sucursal,
						etl.fn_LookupIdCondicionCte( null) as id_condic_subclte,
						etl.fn_LookupIdEmpresa (ods.cod_empresa) as id_empresa,
						etl.fn_LookupIdCliente(ods.cod_cliente) as id_cliente,
						etl.fn_LookupIdTipoServDetalle(ods.cod_servicio) as id_tipo_serv_detalle,
						isnull(sc.id_serv_concepto,-1) as id_serv_concepto,
						etl.fn_LookupIdUsuario(null) as id_usuario,
						cast(null as numeric) as id_oportunidad,
						etl.fn_LookupIdContacto(null) as id_contacto,
						0 as mb_porc,
						0 as margen_bruto_teo,  
						0 as facturacion_cotiz,
						etl.fn_LookupIdServicioPri (null) as id_servicio_pri,
						case when substring(ods.cod_cuenta,1,2) = '60' then isnull(ods.importe,0)*-1 else 0 end as facturacion,
						case when substring(ods.cod_cuenta,1,2) = '65' then isnull(ods.importe,0) else 0 end as costo
					from dbo.ODS_OPR_AJUSTES_EG ods
						left outer join DFAC_LK_SERVICIO_CONCEPTO sc
							on sc.cod_serv_concepto = ods.cod_cuenta
							 and sc.id_tipo_serv_detalle = 0
					where periodo = @idMes
						and substring(ods.cod_cuenta,1,2) in ('60', '65')
					) vw
				group by vw.origen, vw.id_empresa, vw.id_sucursal, vw.id_condic_subclte, vw.id_mes, 
					vw.id_cliente, vw.id_tipo_serv_detalle,  vw.id_serv_concepto

				set @msg = 'Insertados (ADJ): ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg


				-- next
				fetch next from lc_cursor into @idMes
			end
		close lc_cursor
		deallocate lc_cursor

		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'Carga DW:'

		delete from dbo.FT_FACTURACION_SERV_CONCEP
		where id_mes in (select distinct id_mes from stg.PD_FT_FACTURACION_SERV_CONCEP)

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
				
		insert into dbo.FT_FACTURACION_SERV_CONCEP (
			id_empresa, id_sucursal, id_condic_subclte, id_mes,
			id_subcliente, id_cliente, id_tipo_serv_detalle,
			id_serv_concepto, id_usuario, importe_fact, mb_monto, mb_monto_scf,
			origen)
		select id_empresa, id_sucursal, id_condic_subclte, id_mes,
			id_subcliente, id_cliente, id_tipo_serv_detalle,
			id_serv_concepto, id_usuario, 
			sum(importe_fact), 
			sum(mb_monto), 
			sum(mb_monto_scf),
			origen
		from stg.PD_FT_FACTURACION_SERV_CONCEP
		where not (importe_fact = 0
				and mb_monto = 0
				and mb_monto_scf = 0)
		group by id_empresa, id_sucursal, id_condic_subclte, id_mes,
			id_subcliente, id_cliente, id_tipo_serv_detalle,
			id_serv_concepto, id_usuario, origen
		order by id_mes

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.map_ft_facturacion_serv_concep 0