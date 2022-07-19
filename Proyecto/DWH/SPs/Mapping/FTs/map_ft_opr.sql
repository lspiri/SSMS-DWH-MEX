use MX_DWH
Go

if object_id ('etl.map_ft_opr', 'P') is not null
	drop procedure etl.map_ft_opr
GO

create procedure etl.map_ft_opr (
	@idBatch numeric(15),
	@idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31/03/2022
	-- Description:	Mapping FT_OPR
	-- =============================================
	-- 18/07/2022 - Version Inicial
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10)

		-- Inicio
		set @msg = '>>- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Validar Si hay Archivos
		if not exists( select 1 from stg.ST_FT_OPR_DETALLE_CUENTA )
			begin
				set @msg = 'No existe informacion para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return
			end

		-- Mes
		if @idMes > 0
			begin 
				set @msg = 'Id Mes: ' + convert(varchar, @idMes); 
			end
		else
			begin
				select @msg = 'Id Mes: ' + case when count(1) = 0 then '<Ninguno>'
					when count(1) = 1 then convert(varchar, max(idMes))
					else '<Varios Meses Procesados>' end 
				from (select distinct periodo as idMes from stg.ST_FT_OPR_DETALLE_CUENTA) x
			end
		exec etl.prc_Logging @idBatch, @msg

		-------------------------------------------
		-- PREPROCESAMIENTO DATOS (PD)
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'PreProcesamiento de Datos:'
		
		begin transaction

		delete stg.PD_FT_OPR_DETALLE_CUENTA
		where id_mes = @idMes or 
			(@idMes = 0 and id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA))

		set @msg = 'Borrados PD: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		insert into stg.PD_FT_OPR_DETALLE_CUENTA
			(origen, id_mes, id_cc_orig, id_cc_opr, id_sucursal, cod_sucursal, cod_cliente,
			id_cliente, id_subclte, id_empresa, id_linea, cod_linea, 
			id_tipo_serv_detalle, cod_servicio, id_cuenta, cod_cuenta,
			real_orig, budget_orig)
		select 'D365' as origen, a.periodo,
			etl.fn_LookupIdCC_OPR(case when isnull(a.cod_sucursal,'') = '' then isnull(tx.cod_sucursal, '930430') else a.cod_sucursal end) id_cc_orig,
			etl.fn_LookupIdCC_OPR(case when isnull(a.cod_sucursal,'') = '' then isnull(tx.cod_sucursal, '930430') else a.cod_sucursal end) id_cc_opr,
			etl.fn_LookupIdSucursal(case when isnull(a.cod_sucursal,'') = '' then isnull(tx.cod_sucursal, '930430') else a.cod_sucursal end) id_sucursal,
			case when isnull(a.cod_sucursal,'') = '' then isnull(tx.cod_sucursal, '930430') else a.cod_sucursal end as cod_sucursal,
			a.cod_cliente,
			etl.fn_LookupIdCliente(a.cod_cliente) as id_cliente,
			0 as id_subcliente,
			etl.fn_LookupIdEmpresa(a.cod_empresa) id_empresa,
			case when l.id_linea is not null then id_linea
				else case when a.cod_servicio is not null and a.cod_cuenta is not null then
					-1
				else
					0
				end
			end as id_linea,
			l.cod_linea, 
			etl.fn_LookupIdTipoServDetalle(a.cod_servicio) as id_tipo_serv_detalle,
			a.cod_servicio,
			etl.fn_LookupIdCtaContable(a.cod_cuenta) as id_cuenta,
			a.cod_cuenta,
			isnull(a.importe,0) as real_orig,
			0 as budget_orig
		from stg.ST_FT_OPR_DETALLE_CUENTA a
			left outer join dbo.DESG_LK_LINEA l
				on l.cod_cuenta = isnull(a.cod_cuenta,'NA')
					and l.cod_servicio = (case when a.cod_servicio is null or a.cod_servicio = '' then 'NA' else a.cod_servicio end)
			left outer join dbo.ODS_OPR_AJUSTES_CC tx
				on isnull(a.cod_sucursal,'') = '' and tx.cod_cuenta = a.cod_cuenta and tx.periodo = a.periodo
		where 1=1
			and (a.periodo = cast(@idMes as varchar) or @idMes = 0)
			--and a.cod_cliente != '00000'
			and a.posting in ('Tax', 'Current')

		set @msg = 'Insertados PD: ' +  cast(@@rowcount  as varchar); exec etl.prc_Logging @idBatch, @msg

		insert into stg.PD_FT_OPR_DETALLE_CUENTA
			(origen, id_mes, id_cc_orig, id_cc_opr, id_sucursal, cod_sucursal, cod_cliente,
			id_cliente, id_subclte, id_empresa, id_linea, cod_linea, 
			id_tipo_serv_detalle, cod_servicio, id_cuenta, cod_cuenta,
			real_orig, budget_orig)
		select vw.origen, vw.id_mes, vw.id_cc as id_cc_orig, vw.id_cc as id_cc_opr, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, 0 as id_subclte, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta,
			sum(vw.importe) as real_orig, 0 as budget_orig
		from (
			select cast(ods.periodo as numeric) as id_mes,
				ods.tipo as origen,
				etl.fn_LookupIdCC_OPR(ods.cod_sucursal) as id_cc,
				etl.fn_LookupIdSucursal(ods.cod_sucursal) as id_sucursal,
				etl.fn_LookupIdEmpresa (ods.cod_empresa) as id_empresa,
				etl.fn_LookupIdCliente(ods.cod_cliente) as id_cliente,
				etl.fn_LookupIdTipoServDetalle(ods.cod_servicio) as id_tipo_serv_detalle,
				etl.fn_LookupIdCtaContable(ods.cod_cuenta) as id_cuenta,
				ods.cod_sucursal,
				ods.cod_cuenta,
				ods.cod_servicio,
				ods.cod_cliente,
				case when l.id_linea is not null then id_linea
					else case when ods.cod_servicio is not null and ods.cod_cuenta is not null then
						-1
					else
						0
					end
				end as id_linea,
				l.cod_linea,
				ods.importe
			from dbo.ODS_OPR_AJUSTES_EG ods
				left outer join dbo.DESG_LK_LINEA l
					on l.cod_cuenta = isnull(ods.cod_cuenta,'NA')
						and l.cod_servicio = (case when ods.cod_servicio is null or ods.cod_servicio = '' then 'NA' else ods.cod_servicio end) 
			where periodo in (select distinct periodo 
					from stg.ST_FT_OPR_DETALLE_CUENTA
					where periodo = cast(@idMes as varchar) or @idMes = 0)
			) vw
		group by vw.id_mes, vw.origen, vw.id_cc, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta

		set @msg = 'Insertados Ajustes: ' +  cast(@@rowcount  as varchar); exec etl.prc_Logging @idBatch, @msg

		insert into stg.PD_FT_OPR_DETALLE_CUENTA
			(origen, id_mes, id_cc_orig, id_cc_opr, id_sucursal, cod_sucursal, cod_cliente,
			id_cliente, id_subclte, id_empresa, id_linea, cod_linea, 
			id_tipo_serv_detalle, cod_servicio, id_cuenta, cod_cuenta,
			real_orig, budget_orig)
		select vw.origen, vw.id_mes, vw.id_cc as id_cc_orig, vw.id_cc as id_cc_opr, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, 0 as id_subclte, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta,
			0 as real_orig, sum(vw.importe) as budget_orig
		from (
			select cast(ods.periodo as numeric) as id_mes,
				isnull(src.origen, 'D365') as origen,
				etl.fn_LookupIdCC_OPR(ods.cod_sucursal) as id_cc,
				etl.fn_LookupIdSucursal(ods.cod_sucursal) as id_sucursal,
				etl.fn_LookupIdEmpresa (ods.cod_empresa) as id_empresa,
				etl.fn_LookupIdCliente(ods.cod_cliente) as id_cliente,
				etl.fn_LookupIdTipoServDetalle(ods.cod_servicio) as id_tipo_serv_detalle,
				etl.fn_LookupIdCtaContable(ods.cod_cuenta) as id_cuenta,
				ods.cod_sucursal,
				ods.cod_cuenta,
				ods.cod_servicio,
				ods.cod_cliente,
				case when l.id_linea is not null then id_linea
					else case when ods.cod_servicio is not null and ods.cod_cuenta is not null then
						-1
					else
						0
					end
				end as id_linea,
				l.cod_linea,
				ods.importe
			from dbo.ODS_OPR_BUDGET ods
				left outer join dbo.DESG_LK_LINEA l
					on l.cod_cuenta = isnull(ods.cod_cuenta,'NA')
						and l.cod_servicio = (case when ods.cod_servicio is null or ods.cod_servicio = '' then 'NA' else ods.cod_servicio end) 
				left outer join (select distinct id_mes, cod_empresa, cod_sucursal, cod_cliente, cod_servicio, cod_cuenta, origen
							from (
								select st.id_mes, em.cod_empresa, st.cod_sucursal, st.cod_cliente, 
									case when rtrim(isnull(st.cod_servicio,'')) = '' then 'NA' else cod_servicio end as cod_servicio, st.cod_cuenta,
									st.origen,
									rank() over (partition by st.id_mes, em.cod_empresa, st.cod_sucursal, st.cod_cliente, 
										case when rtrim(isnull(st.cod_servicio,'')) = '' then 'NA' else cod_servicio end, st.cod_cuenta order by 
									case when origen = 'D365' then 1 when origen = 'Reclas FRT Package' then 2 else 3 end) as rnk
								from stg.PD_FT_OPR_DETALLE_CUENTA st,
									dbo.DEMP_LK_EMPRESA em
								where st.id_empresa = em.id_empresa) vw
							where rnk = 1) src
					on src.id_mes = ods.periodo
						and src.cod_empresa = ods.cod_empresa
						and src.cod_sucursal = ods.cod_sucursal
						and src.cod_cliente = ods.cod_cliente
						and src.cod_servicio = ods.cod_servicio
						and src.cod_cuenta = ods.cod_cuenta 
			where periodo in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
				where periodo = cast(@idMes as varchar) or @idMes = 0)
			) vw
		group by vw.id_mes, vw.origen, vw.id_cc, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta

		set @msg = 'Insertados Budget: ' +  cast(@@rowcount  as varchar); exec etl.prc_Logging @idBatch, @msg
		
		insert into stg.PD_FT_OPR_DETALLE_CUENTA
			(origen, id_mes, id_cc_orig, id_cc_opr, id_sucursal, cod_sucursal, cod_cliente,
			id_cliente, id_subclte, id_empresa, id_linea, cod_linea, 
			id_tipo_serv_detalle, cod_servicio, id_cuenta, cod_cuenta,
			real_orig, budget_orig)
		select vw.origen, vw.id_mes, vw.id_cc as id_cc_orig, vw.id_cc as id_cc_opr, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, 0 as id_subclte, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta,
			max(vw.importe) as real_orig, 0 as budget_orig
		from (
			select distinct ve.id_mes,
				'DSO' as origen,
				ve.id_cc,
				ve.id_sucursal,
				ve.id_empresa,
				ve.id_cliente,
				0 as id_tipo_serv_detalle,
				0 as id_cuenta,
				ve.cod_sucursal,
				l.cod_cuenta,
				l.cod_servicio,
				ve.cod_cliente,
				l.id_linea,
				l.cod_linea,
				ods.dso as importe
			-- select distinct ve.*, cco.cod_agrupador, ods.dso
			from (select pd.id_mes, pd.id_cc_orig as id_cc, pd.id_sucursal, pd.cod_sucursal, pd.cod_cliente,
						pd.id_cliente, pd.id_subclte, pd.id_empresa, em.cod_empresa, sum(pd.real_orig) as venta
					-- select *
					from stg.PD_FT_OPR_DETALLE_CUENTA pd,
						dbo.DEMP_LK_EMPRESA em
					where pd.id_empresa = em.id_empresa
						and pd.id_mes in (select distinct periodo 
								from stg.ST_FT_OPR_DETALLE_CUENTA
								where periodo = cast(@idMes as varchar) or @idMes = 0)
						and pd.cod_cuenta like '60%'
					group by pd.id_mes, pd.id_cc_orig, pd.id_cc_opr, pd.id_sucursal, pd.cod_sucursal, pd.cod_cliente,
						pd.id_cliente, pd.id_subclte, pd.id_empresa, em.cod_empresa
					having sum(pd.real_orig) != 0) ve,
				dbo.VW_LK_CC_OPR_HIST cco,
				dbo.ODS_OPR_DSO ods,
				(select ln.*
					from dbo.DESG_LK_LINEA ln
					where ln.cod_cuenta = '990369'
						and cod_servicio = 'NA') l -- Payment Terms
			where ve.id_mes = cast(cco.id_mes as numeric)
				and ve.cod_sucursal = cco.cod_cc
				and cco.id_mes = ods.periodo
				and cco.cod_agrupador = ods.cod_sucursal
				and ve.cod_empresa = ods.cod_empresa
				--and cco.cod_agrupador = '101030' and ods.cod_empresa = '1181'
				--and cco.cod_agrupador = '101072' and ods.cod_empresa = '1181'
				
			) vw
		group by vw.id_mes, vw.origen, vw.id_cc, vw.id_sucursal, vw.cod_sucursal, vw.cod_cliente,
			vw.id_cliente, vw.id_empresa, vw.id_linea, vw.cod_linea, 
			vw.id_tipo_serv_detalle, vw.cod_servicio, vw.id_cuenta, vw.cod_cuenta

		set @msg = 'Insertados DSO: ' +  cast(@@rowcount  as varchar); exec etl.prc_Logging @idBatch, @msg


		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'Carga DW:'
				
		delete dbo.FT_OPR_DETALLE_CUENTA 
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
						where periodo = cast(@idMes as varchar) or @idMes = 0)
		set @msg = 'Borrados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg	
		
		delete dbo.FT_OPR_AGRUPADA
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
						where periodo = cast(@idMes as varchar) or @idMes = 0)
		set @msg = 'Borrados FT Agr: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 
				
		delete dbo.FT_OPR_DETALLE 
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
						where periodo = cast(@idMes as varchar) or @idMes = 0)
		set @msg = 'Borrados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		delete dbo.FT_OPR 
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
						where periodo = cast(@idMes as varchar) or @idMes = 0)
		set @msg = 'Borrados FT: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 
		
		-- Detalle Cuenta
		insert into dbo.FT_OPR_DETALLE_CUENTA (origen, id_mes, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget)
		select origen, id_mes, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			sum(real_orig) as real,
			sum(budget_orig) as budget
		from stg.PD_FT_OPR_DETALLE_CUENTA
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
						where periodo = cast(@idMes as varchar) or @idMes = 0)
		group by origen, id_mes, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle
		having not (sum(real_orig) = 0 and sum(budget_orig) = 0)

		set @msg = 'Insertados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Detalle Cuenta
		insert into dbo.FT_OPR_AGRUPADA (origen, id_mes, 
			id_cc_opr, id_agrupador, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget, real_num, real_den, division)
		select pd.origen, pd.id_mes, 
			pd.id_cc_opr, cc.id_agrupador, pd.id_empresa, 
			pd.id_linea, pd.id_cuenta, pd.id_tipo_serv_detalle,
			case when pd.origen = 'DSO' then avg(pd.real_orig) else sum(pd.real_orig) end as real,
			sum(pd.budget_orig) as budget,
			0 as real_num, 
			0 as real_den, 
			0 as division
		from stg.PD_FT_OPR_DETALLE_CUENTA pd,
			dbo.VW_LK_CC_OPR_HIST cc
		where pd.id_cc_opr = cc.id_cc
			and pd.id_mes = cc.id_mes
			and pd.id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
							where periodo = cast(@idMes as varchar) or @idMes = 0)
		group by pd.origen, pd.id_mes, 
			pd.id_cc_opr, cc.id_agrupador, pd.id_empresa, 
			pd.id_linea, pd.id_cuenta, pd.id_tipo_serv_detalle
		having not (sum(pd.real_orig) = 0 and sum(pd.budget_orig) = 0)

		set @msg = 'Insertados FT Agr: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Detalle
		insert into dbo.FT_OPR_DETALLE (origen, id_mes, id_cc_orig, id_cc_opr, id_cliente, 
			id_subclte, id_empresa, id_linea, 
			real, budget)
		select origen, id_mes, id_cc_orig, id_cc_opr, id_cliente, 
			id_subclte, id_empresa, id_linea,
			case when pd.origen = 'DSO' then avg(pd.real_orig) else sum(pd.real_orig) end as real,
			sum(budget_orig) as budget
		from stg.PD_FT_OPR_DETALLE_CUENTA pd
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
					where periodo = cast(@idMes as varchar) or @idMes = 0)
		group by origen, id_mes, id_cc_orig, id_cc_opr, id_cliente, 
			id_subclte, id_empresa, id_linea
		having not (sum(real_orig) = 0 and sum(budget_orig) = 0)

		set @msg = 'Insertados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		-- OPT
		insert into FT_OPR (origen, id_mes, id_cc_orig, id_cc_opr, 
			id_linea, real, budget)
		select origen, id_mes, id_cc_orig, id_cc_opr, id_linea,
			case when pd.origen = 'DSO' then avg(pd.real_orig) else sum(pd.real_orig) end as real,
			sum(budget_orig) as budget
		from stg.PD_FT_OPR_DETALLE_CUENTA pd
		where id_mes in (select distinct periodo from stg.ST_FT_OPR_DETALLE_CUENTA
					where periodo = cast(@idMes as varchar) or @idMes = 0)
		group by origen, id_mes, id_cc_orig, id_cc_opr, id_linea
		having not (sum(real_orig) = 0 and sum(budget_orig) = 0)

		set @msg = 'Insertados FT: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 
	
		commit transaction

	end try
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

	end catch
end
GO
-- exec etl.map_ft_opr 0, 202204