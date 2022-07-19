use MX_DWH
Go

if object_id ('etl.map_ft_facturacion_ts', 'P') is not null
	drop procedure etl.map_ft_facturacion_ts
GO

create procedure etl.map_ft_facturacion_ts (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 25/03/2022
	-- Description:	Mapping FT Facturacion TS
	-- =============================================
	-- 13/06/2022 - Version Inicial


	begin try
		set nocount on

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@idMes numeric(10)

	
		-- Inicio
		set @msg = '>>- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

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

		-- Exclusion Servicion Especializados
		delete st
		-- select *
		from stg.ST_FT_FACTURACION_TS st
		where 1=1
			--and tipo_operacion = 'VTA'
			and periodo >= '202101' and periodo < '202109'
			and cod_servicio in (select cod_tipo_serv_detalle as cod_servicio
						from dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sd,
							dbo.DFAC_LK_SERVICIO2 s2
						where sd.id_tipo_servicio2  =s2.id_tipo_servicio2
							and s2.desc_tipo_servicio2 = 'Serv. Especializado')

		set @msg = 'Exclusion (Srv. Exp.): ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

		delete from stg.PD_FT_FACTURACION_TS
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

				insert into stg.PD_FT_FACTURACION_TS (id_stage, 
					id_mes, id_sucursal, id_condic_subclte, id_cliente, 
					id_tipo_serv_detalle, id_subcliente, id_empresa, 
					importe_fact, mb_monto, mb_porc,
					id_usuario, id_oportunidad, id_contacto,
					margen_bruto_teo, facturacion_cotiz, id_servicio_pri,
					origen)
				select max(id_stage), 
					id_mes, id_sucursal, id_condic_subclte, id_cliente, 
					id_tipo_serv_detalle, id_subcliente, id_empresa, 
					sum(facturacion) as importe_fact, 
					(Sum(facturacion)-Sum(costo)) as mb_monto,
					sum(mb_porc), 
					max(id_usuario), 
					max(id_oportunidad), 
					max(id_contacto),
					sum(margen_bruto_teo), 
					sum(facturacion_cotiz), 
					max(id_servicio_pri),
					'D365' as origen
				from (
					select id_stage,
						cast(a.periodo as numeric) as id_mes, 
						etl.fn_LookupIdSucursal(a.cod_sucursal) as id_sucursal,
						etl.fn_LookupIdCondicionCte( ods.cod_condicion_final ) as id_condic_subclte,
						isnull(b.id_cliente,-1) as id_cliente,
						etl.fn_LookupIdTipoServDetalle(a.cod_servicio) as id_tipo_serv_detalle,
						etl.fn_LookupIdSubcliente(null, null) as id_subcliente,
						etl.fn_LookupIdEmpresa(a.cod_empresa) as id_empresa,
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
						-- select count(1)
					from stg.ST_FT_FACTURACION_TS a
						left outer join dbo.DCTE_LK_CLIENTE b 
							on b.cod_cliente = a.cod_cliente
						left outer join dbo.ODS_CNRP ods
							on ods.periodo = a.periodo
								and ods.cod_cliente = a.cod_cliente
								and ods.cod_sucursal = a.cod_sucursal
								and ods.cod_servicio = a.cod_servicio
								and ods.cod_empresa = 'NA'
					where a.periodo = @idMes
						and a.tipo_operacion in ('VTA', 'COS')
					) vw
				group by id_mes, id_sucursal, id_condic_subclte, id_cliente, 
						id_tipo_serv_detalle, id_subcliente, id_empresa

				set @msg = 'Insertados (D365): ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

				insert into stg.PD_FT_FACTURACION_TS (id_stage, 
					id_mes, id_sucursal, id_condic_subclte, id_cliente, 
					id_tipo_serv_detalle, id_subcliente, id_empresa, 
					importe_fact, mb_monto, mb_porc,
					id_usuario, id_oportunidad, id_contacto,
					margen_bruto_teo, facturacion_cotiz, id_servicio_pri,
					origen)
				select 0 as id_stage, vw.id_mes, vw.id_sucursal, vw.id_condic_subclte, vw.id_cliente, 
					vw.id_tipo_serv_detalle, 0 as id_subcliente, vw.id_empresa, 
					sum(facturacion) as importe_fact, 
					(Sum(facturacion)-Sum(costo)) as mb_monto,
					sum(mb_porc) as mb_porc, 
					max(id_usuario) as id_usuario, 
					max(id_oportunidad) as id_oportunidad, 
					max(id_contacto) as id_contacto,
					sum(margen_bruto_teo) as margen_bruto_teo, 
					sum(facturacion_cotiz) as facturacion_cotiz, 
					max(id_servicio_pri) as id_servicio_pri,
					vw.origen
				from (
					select cast(ods.periodo as numeric) as id_mes,
						ods.tipo as origen,
						etl.fn_LookupIdSucursal(ods.cod_sucursal) as id_sucursal,
						etl.fn_LookupIdCondicionCte( c.cod_condicion_final ) as id_condic_subclte,
						etl.fn_LookupIdEmpresa (ods.cod_empresa) as id_empresa,
						etl.fn_LookupIdCliente(ods.cod_cliente) as id_cliente,
						etl.fn_LookupIdTipoServDetalle(ods.cod_servicio) as id_tipo_serv_detalle,
						etl.fn_LookupIdUsuario(null) as id_usuario,
						cast(null as numeric) as id_oportunidad,
						etl.fn_LookupIdContacto(null) as id_contacto,
						0 as mb_porc,
						0 as margen_bruto_teo,  
						0 as facturacion_cotiz,
						etl.fn_LookupIdServicioPri (null) as id_servicio_pri,
						case when substring(ods.cod_cuenta,1,2) = '60' then isnull(ods.importe,0)*-1 else 0 end as facturacion,
						case when substring(ods.cod_cuenta,1,2) = '65' then isnull(ods.importe,0) else 0 end as costo
						-- select count(1)
					from dbo.ODS_OPR_AJUSTES_EG ods
						left outer join dbo.ODS_CNRP c
							on c.periodo = ods.periodo
								and c.cod_cliente = ods.cod_cliente
								and c.cod_sucursal = ods.cod_sucursal
								and c.cod_servicio = ods.cod_servicio
								and c.cod_empresa = 'NA'
					where ods.periodo = @idMes
						and substring(ods.cod_cuenta,1,2) in ('60', '65')
					) vw
				group by vw.id_mes, vw.origen, vw.id_sucursal, vw.id_condic_subclte, vw.id_cliente, 
					vw.id_tipo_serv_detalle, vw.id_empresa

				set @msg = 'Insertados (ADJ): ' + cast (@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg


				-- next
				fetch next from lc_cursor into @idMes
			end
		close lc_cursor
		deallocate lc_cursor

		-- Recalculo el % MB
		update stg.PD_FT_FACTURACION_TS
		set mb_porc = round(mb_monto / importe_fact,3)
		where importe_fact <> 0

		set @msg = 'Calculo de % MB: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, '- Carga DW:'
	
		-- Borrar
		delete from dbo.FT_FACTURACION_TS
		where id_mes in (select distinct id_mes from stg.PD_FT_FACTURACION_TS)

		set @msg = 'Borrados (FT): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar
		insert into dbo.FT_FACTURACION_TS (id_empresa, id_sucursal, id_condic_subclte,
			id_mes, id_subcliente, id_tipo_serv_detalle,
			id_cliente, importe_fact, mb_porc, mb_monto,
			id_usuario, MB_Monto_SCF, id_contacto,
			margen_bruto_teo, facturacion_cotiz, id_servicio_pri,
			origen)
		select id_empresa, 
			case when isnull(id_sucursal,-1) = -1 then 0 else id_sucursal end as id_sucursal, 
			id_condic_subclte,
			id_mes, id_subcliente, id_tipo_serv_detalle, 
			id_cliente, 
			sum(importe_fact), 
			sum(mb_porc), 
			sum(mb_monto),
			id_usuario, 
			sum(isnull(MB_Monto_SCF,0)),
			max(isnull(id_contacto,0)),
			max(isnull(margen_bruto_teo,0)), 
			max(isnull(facturacion_cotiz,0)),
			max(isnull(id_servicio_pri,0)),
			origen
		from stg.PD_FT_FACTURACION_TS
		where not (importe_fact = 0
				and mb_porc = 0
				and mb_monto = 0
				and mb_monto_scf = 0)
		group by id_empresa, id_sucursal, id_condic_subclte,
			id_mes, id_subcliente, id_tipo_serv_detalle, 
			id_cliente, id_usuario, origen

		set @msg = 'Insertados (FT): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		commit transaction

		-- OK
		return 1

	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch
	end catch
end
GO
-- exec etl.map_ft_facturacion_ts 0