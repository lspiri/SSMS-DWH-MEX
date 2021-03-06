USE [MX_DWH]
GO
/****** Object:  StoredProcedure [etl].[prc_VerificarEjecucionJob]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_VerificarEjecucionJob]
GO
/****** Object:  StoredProcedure [etl].[prc_run_secuencia]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_run_secuencia]
GO
/****** Object:  StoredProcedure [etl].[prc_run_proceso]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_run_proceso]
GO
/****** Object:  StoredProcedure [etl].[prc_run_paquete_dts]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_run_paquete_dts]
GO
/****** Object:  StoredProcedure [etl].[prc_run_control_isk]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_run_control_isk]
GO
/****** Object:  StoredProcedure [etl].[prc_run_control_datos]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_run_control_datos]
GO
/****** Object:  StoredProcedure [etl].[prc_reg_periodo_actual]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_reg_periodo_actual]
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_varios]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_reg_ctrl_varios]
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_mapping]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_reg_ctrl_mapping]
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_isk_FT]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_reg_ctrl_isk_FT]
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_datos]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_reg_ctrl_datos]
GO
/****** Object:  StoredProcedure [etl].[prc_Logging]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_Logging]
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_sp]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_log_error_sp]
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_seq]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_log_error_seq]
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_dts]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_log_error_dts]
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch_mail]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_log_batch_mail]
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_log_batch]
GO
/****** Object:  StoredProcedure [etl].[prc_iniciar_proceso]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_iniciar_proceso]
GO
/****** Object:  StoredProcedure [etl].[prc_gen_verificacion_df]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_gen_verificacion_df]
GO
/****** Object:  StoredProcedure [etl].[prc_gen_batch_secuencia]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_gen_batch_secuencia]
GO
/****** Object:  StoredProcedure [etl].[prc_finalizar_proceso]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_finalizar_proceso]
GO
/****** Object:  StoredProcedure [etl].[prc_exportar_isk]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_exportar_isk]
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_usuario]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_enviar_mail_usuario]
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_secuencia]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_enviar_mail_secuencia]
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_operativo]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_enviar_mail_operativo]
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_control]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_enviar_mail_control]
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_job]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_ctrl_verificacion_job]
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_df]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[prc_ctrl_verificacion_df]
GO
/****** Object:  StoredProcedure [etl].[carga_lks_tiempo_anual]    Script Date: 17/03/2022 12:40:21 ******/
DROP PROCEDURE [etl].[carga_lks_tiempo_anual]
GO
/****** Object:  View [dbo].[VW_INF_TRACKING_LKAM_CHG]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[VW_INF_TRACKING_LKAM_CHG]
GO
/****** Object:  View [dbo].[VW_FT_WIN_RATE_GANADAS]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[VW_FT_WIN_RATE_GANADAS]
GO
/****** Object:  View [dbo].[VW_FT_CONVERSION_RATE]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[VW_FT_CONVERSION_RATE]
GO
/****** Object:  View [dbo].[REL_CNRP_TS_ACTUAL]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[REL_CNRP_TS_ACTUAL]
GO
/****** Object:  View [dbo].[REL_CNRP_SUCURSAL_ACTUAL]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[REL_CNRP_SUCURSAL_ACTUAL]
GO
/****** Object:  View [dbo].[REL_CNRP_PAIS_ACTUAL]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[REL_CNRP_PAIS_ACTUAL]
GO
/****** Object:  View [dbo].[FT_FACTURACION_SERV_CONCEP_CP]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[FT_FACTURACION_SERV_CONCEP_CP]
GO
/****** Object:  View [dbo].[FT_FACTURACION_CPERDIDOS]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[FT_FACTURACION_CPERDIDOS]
GO
/****** Object:  View [dbo].[FT_FACTURACION_CP_PAIS]    Script Date: 17/03/2022 12:40:21 ******/
DROP VIEW [dbo].[FT_FACTURACION_CP_PAIS]
GO
/****** Object:  View [dbo].[FT_FACTURACION_CP_PAIS]    Script Date: 17/03/2022 12:40:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[FT_FACTURACION_CP_PAIS]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[FT_FACTURACION_CP_PAIS] WITH SCHEMABINDING AS
select rel.ID_MES, ft.ID_EMPRESA, ft.ID_SUCURSAL, rel.id_condicion as ID_CONDIC_SUBCLTE, 
	ft.ID_TIPO_SERV_DETALLE, ft.ID_CLIENTE,
	sum(isnull(ft.importe_fact,0)) as Importe_Fact_CP_PAIS,
	sum(isnull(ft.mb_monto,0)) as MB_Monto_CP_PAIS,
	COUNT_BIG(*) as Cantidad_CP_PAIS
from dbo.FT_FACTURACION_TS ft,
	dbo.REL_CNRP_PAIS rel, 
	dbo.DCTE_LK_CONDICION lk,
	dbo.DTPO_LK_MESES mes
where ft.id_mes = mes.id_mes_anio_ant
	and ft.id_cliente = rel.id_cliente
	--and ft.id_sucursal = rel.id_sucursal
	--and ft.id_tipo_serv_detalle = rel.id_tipo_serv_detalle
	and rel.id_condicion = lk.id_condicion
	and rel.id_mes = mes.id_mes
	and lk.cod_condicion = 'P'
group by rel.id_mes, ft.id_empresa, ft.id_sucursal, rel.id_condicion, 
	ft.id_tipo_serv_detalle, ft.id_cliente
GO
/****** Object:  View [dbo].[FT_FACTURACION_CPERDIDOS]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[FT_FACTURACION_CPERDIDOS]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[FT_FACTURACION_CPERDIDOS] WITH SCHEMABINDING AS
select rel.ID_MES, ft.ID_EMPRESA, ft.ID_SUCURSAL, rel.id_condicion as ID_CONDIC_SUBCLTE, 
	ft.ID_TIPO_SERV_DETALLE, ft.ID_CLIENTE, ft.ID_USUARIO,
	sum(isnull(ft.importe_fact,0)) as Importe_Fact_CP,
	sum(isnull(ft.mb_monto,0)) as MB_Monto_CP,
	COUNT_BIG(*) as Cantidad_CP
from dbo.FT_FACTURACION_TS ft,
	dbo. REL_CNRP_TS rel, 
	dbo.DCTE_LK_CONDICION lk,
	dbo.DTPO_LK_MESES mes
where ft.id_mes = mes.id_mes_anio_ant
	and ft.id_cliente = rel.id_cliente
	and ft.id_sucursal = rel.id_sucursal
	and ft.id_tipo_serv_detalle = rel.id_tipo_serv_detalle
	and rel.id_condicion = lk.id_condicion
	and rel.id_mes = mes.id_mes
	and lk.cod_condicion = 'P'
group by rel.id_mes, ft.id_empresa, ft.id_sucursal, rel.id_condicion, 
	ft.id_tipo_serv_detalle, ft.id_cliente, ft.ID_USUARIO

GO
/****** Object:  View [dbo].[FT_FACTURACION_SERV_CONCEP_CP]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[FT_FACTURACION_SERV_CONCEP_CP]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[FT_FACTURACION_SERV_CONCEP_CP] WITH SCHEMABINDING AS
select rel.ID_MES, ft.ID_EMPRESA, ft.ID_SUCURSAL, rel.id_condicion as ID_CONDIC_SUBCLTE, 
	ft.ID_TIPO_SERV_DETALLE, ft.ID_CLIENTE, ft.ID_USUARIO, ft.ID_SERV_CONCEPTO, 
	sum(isnull(ft.importe_fact,0)) as Importe_Fact_CP,
	sum(isnull(ft.mb_monto,0)) as MB_Monto_CP,
	COUNT_BIG(*) as Cantidad_CP
from dbo.FT_FACTURACION_SERV_CONCEP ft,
	dbo. REL_CNRP_TS rel, 
	dbo.DCTE_LK_CONDICION lk,
	dbo.DTPO_LK_MESES mes
where ft.id_mes = mes.id_mes_anio_ant
	and ft.id_cliente = rel.id_cliente
	and ft.id_sucursal = rel.id_sucursal
	and ft.id_tipo_serv_detalle = rel.id_tipo_serv_detalle
	and rel.id_condicion = lk.id_condicion
	and rel.id_mes = mes.id_mes
	and lk.cod_condicion = 'P'
group by rel.id_mes, ft.id_empresa, ft.id_sucursal, rel.id_condicion, 
	ft.id_tipo_serv_detalle, ft.id_cliente, ft.ID_USUARIO, ft.ID_SERV_CONCEPTO
GO
/****** Object:  View [dbo].[REL_CNRP_PAIS_ACTUAL]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[REL_CNRP_PAIS_ACTUAL]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[REL_CNRP_PAIS_ACTUAL] WITH SCHEMABINDING AS
select REL.ID_CLIENTE, REL.ID_CONDICION 
from dbo.REL_CNRP_PAIS REL,
	dbo.ODS_TM_PERIODO_ACTUAL UPP
where REL.id_mes = UPP.id_periodo
	and UPP.tabla_modelo = 'FT_FACTURACION_TS'
GO
/****** Object:  View [dbo].[REL_CNRP_SUCURSAL_ACTUAL]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[REL_CNRP_SUCURSAL_ACTUAL]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[REL_CNRP_SUCURSAL_ACTUAL] WITH SCHEMABINDING AS
select REL.ID_CLIENTE,  REL.ID_SUCURSAL, REL.ID_CONDICION  
from dbo.REL_CNRP_SUCURSAL REL,
	dbo.ODS_TM_PERIODO_ACTUAL UPP
where REL.id_mes = UPP.id_periodo
	and UPP.tabla_modelo = 'FT_FACTURACION_TS'
GO
/****** Object:  View [dbo].[REL_CNRP_TS_ACTUAL]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[REL_CNRP_TS_ACTUAL]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE VIEW [dbo].[REL_CNRP_TS_ACTUAL] WITH SCHEMABINDING AS
select  REL.ID_CLIENTE, REL.ID_SUCURSAL, REL.ID_TIPO_SERV_DETALLE, REL.ID_CONDICION,
	REL.MARCA_CLTE_NUEVO
from dbo.REL_CNRP_TS REL,
	dbo.ODS_TM_PERIODO_ACTUAL UPP
where REL.id_mes = UPP.id_periodo
	and UPP.tabla_modelo = 'FT_FACTURACION_TS'
GO
/****** Object:  View [dbo].[VW_FT_CONVERSION_RATE]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[VW_FT_CONVERSION_RATE] as
select vw.id_mes as ID_MES, 
	vw.id_sucursal as ID_SUCURSAL,
	vw.id_tipo_serv_detalle as ID_TIPO_SERV_DETALLE,
	vw.nro_oportunidad as NRO_OPORTUNIDAD, 
	sum(vw.cant_opp) as Opp_Preap,
	sum(vw.cant_fact) as OPP_Fact
from (
select	opp.id_sucursal,
	opp.id_tipo_serv_detalle,
	opp.nro_oportunidad,
	d.id_mes,
	1 as cant_opp,
	isnull(
		(select max(1) from dbo.FT_PRODUCTIVIDAD_COMERCIAL pc
		where pc.id_sucursal = opp.id_sucursal
			and pc.id_tipo_serv_detalle = opp.id_tipo_serv_detalle
			and pc.nro_cotizacion = opp.nro_cotizacion
			and pc.nro_oportunidad = opp.nro_oportunidad
			and (pc.id_periodo_facturacion >= d.id_mes 
				or pc.id_fecha_facturacion >= opp.id_fecha_creacion_opp) ) ,0) as cant_fact
from dbo.FT_OPORTUNIDADES opp,
	dbo.DTPO_LK_DIAS d
where 1=1
	and d.id_mes >= 201901
	and opp.id_fecha_creacion_opp = d.id_dia
	and opp.id_plan_comercial_opp in (0, 5, 17, 45)
	and opp.acuerdo is null
	and opp.id_tipo_opp in ('2', '1')
	and opp.id_tipo_reg in ('2')
	and opp.prop_enviada in (1)  )  vw
group by vw.id_sucursal,
	vw.id_tipo_serv_detalle,
	vw.nro_oportunidad,
	vw.id_mes 
GO
/****** Object:  View [dbo].[VW_FT_WIN_RATE_GANADAS]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[VW_FT_WIN_RATE_GANADAS] as
select vw.id_mes as ID_MES, 
	vw.id_sucursal as ID_SUCURSAL,
	vw.id_tipo_serv_detalle as ID_TIPO_SERV_DETALLE,
	vw.nro_oportunidad as NRO_OPORTUNIDAD, 
	sum(vw.cant_opp) as Opp_Preap,
	sum(vw.opp_ganadas) as Opp_Ganadas
from (
	select	opp.id_sucursal,
		opp.id_tipo_serv_detalle,
		opp.nro_oportunidad,
		d.id_mes,
		1 as cant_opp,
		case when st.cod_estado_opp = 'Cerrado Ganado' then 1 else 0 end as opp_ganadas
	from dbo.FT_OPORTUNIDADES opp,
		dbo.DTPO_LK_DIAS d,
		dbo.DOPP_LK_ESTADO_OPP st
	where 1=1
		and d.id_mes >= 201901
		and opp.id_fecha_creacion_opp = d.id_dia
		and opp.id_estado_opp = st.id_estado_opp
		and opp.id_plan_comercial_opp in (0, 5, 17, 45)
		and opp.acuerdo is null
		and opp.id_tipo_opp in ('2', '1')
		and opp.id_tipo_reg in ('2')
		and opp.prop_enviada in (1)  )  vw
group by vw.id_sucursal,
	vw.id_tipo_serv_detalle,
	vw.nro_oportunidad,
	vw.id_mes 
GO
/****** Object:  View [dbo].[VW_INF_TRACKING_LKAM_CHG]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  View [dbo].[VW_INF_TRACKING_LKAM_CHG]    Script Date: 20/12/2021 05:47:08 p.m. ******/
create view [dbo].[VW_INF_TRACKING_LKAM_CHG] WITH SCHEMABINDING AS
select A.ID_SEMANA, A.ID_CLIENTE, A.ID_LISTA, A.OWNERNAME, A.CUIT,
	max(C.RAZON_SOCIAL) as RAZON_SOCIAL, 
	max(D.DESC_LISTA) as DESC_LISTA,
	COUNT_BIG(*) as Cant_Regs
from dbo.INF_TRACKING_LKAM a,
	dbo.DCTE_LK_CLIENTE c,
	dbo.DCTE_LK_DGC d,
	dbo.DTPO_LK_SEMANAS s
where a.id_cliente = c.id_cliente
	and a.id_lista = d.id_lista
	and s.id_semana = a.id_semana
	and a.kam = 1
	and not exists (select 1 
				FROM dbo.INF_TRACKING_LKAM b
				where  b.kam = 1
					and b.id_semana = s.id_semana_ant
					and b.id_cliente = a.id_cliente
					and b.id_lista = a.id_lista)
group by A.ID_SEMANA, A.ID_CLIENTE, A.ID_LISTA, A.OWNERNAME, A.CUIT
GO
/****** Object:  StoredProcedure [etl].[carga_lks_tiempo_anual]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[carga_lks_tiempo_anual]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[carga_lks_tiempo_anual] (
	@idBatch numeric(15),
	@anio int)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 16-06-2014
	-- Description:	Mapping LKs Tiempo Anual
	-- =============================================
	-- 16/06/2014 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(255)
		declare @corte bit,	@ult_nro_sem_anio_ant int,
			@fec_corte datetime, @fec_hasta_anio datetime,
			@desde datetime, @hasta datetime,
			@nro_semana int, @nro_mes int, @semana_mes int,
			@fecha_ref datetime, @fecha datetime

		SET DATEFIRST 7 -- Primer Dia de Semana Domingo
		SET DATEFORMAT dmy

		-- Inicio
		set @msg = '>> Proceso: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'AÑO: ' + cast(@anio as varchar); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		-- Borrar LKs del Anio
		if exists (select 1 from dtpo_lk_anio where anio = @anio )
			begin
				delete from dbo.dtpo_lk_dias 
				where id_mes >= @anio * 100 + 1

				delete from dbo.dtpo_lk_semanas 
				where id_mes >= @anio * 100 + 1

				delete from dbo.dtpo_lk_meses
				where id_mes >= @anio * 100 + 1

				delete from dbo.dtpo_lk_trimestre
				where id_trimestre >= @anio * 100 + 1

				delete from dbo.dtpo_lk_semestre
				where anio >= @anio

				delete from dbo.dtpo_lk_anio 
				where anio >= @anio

				set @msg = '** Año Borrado: '; exec etl.prc_Logging @idBatch, @msg
			end

		-- AÑO 
		---------------------------
		insert into dtpo_lk_anio (anio, anio_ant) values (@anio, @anio - 1)
		set @msg = 'Año Insertado: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- SEMESTRE
		---------------------------
		insert into dtpo_lk_semestre (id_semestre, desc_semestre,
				id_sem_ant, id_sem_anio_ant, anio) 
		select anio * 100 + nro as id_semestre,
			'0' + convert(varchar, nro) + '-' + convert(varchar, anio) as desc_semestre,
			case nro when 1 then (anio - 1) * 100 + 2 else anio * 100 + nro - 1 end as id_sem_ant,
			(anio - 1) * 100 + nro as id_sem_anio_ant,
			anio
		from (select @anio as anio, nro from etl.fn_genTabla(1, 2, 1)) lk
		set @msg = 'Semestres Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- TRIMESTRE 
		---------------------------
		insert into dtpo_lk_trimestre (id_trimestre, desc_trimestre,
					id_trim_ant, id_trim_anio_ant, id_semestre)
		select anio * 100 + nro as id_trimestre,
			'0' + convert(varchar, nro) + '-' + convert(varchar, anio) as desc_trimestre,
			case nro when 1 then (anio - 1) * 100 + 4 else anio * 100 + nro - 1 end as id_trim_ant,
			(anio - 1) * 100 + nro as id_trim_anio_ant,
			anio * 100 + case when nro in (1,2) then 1 else 2 end as id_semestre
		from (select @anio as anio, nro from etl.fn_genTabla(1, 4, 1)) lk
		set @msg = 'Trimestres Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- MES
		---------------------------
		insert into dtpo_lk_meses (id_mes, nro_mes,
			desc_mes, id_mes_ant, id_mes_anio_ant, 
			id_trimestre, anio, desc_nro_mes)
		select anio * 100 + nro as id_mes,
			nro as nro_mes,
			substring(etl.fn_getDescMes(nro),1,3) + '-' + cast(anio as varchar) as desc_mes,
			case nro when 1 then (anio - 1) * 100 + 12 else anio * 100 + nro - 1 end as id_mes_ant,
			(anio - 1) * 100 + nro as id_mes_anio_ant,
			((anio ) * 100) + 
					(case when nro <= 3 then 1 when nro <= 6 then 2 when nro <= 9 then 3 else 4
					end) as id_trimestre,
			anio,
			etl.fn_getDescMes(nro)
		from (select @anio as anio, nro from etl.fn_genTabla(1, 12, 1)) lk
		set @msg = 'Meses Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- SEMANAS
		---------------------------
		declare @tblSemanas table (
			anio int,
			nro_semana int,
			fec_desde datetime,
			fec_hasta datetime,
			nro_mes int,
			semana_mes int
		)

		-- Semana Año Anterior (Ref.)
		select @ult_nro_sem_anio_ant = nro_semana_anio, 
			@fecha_ref = DateAdd(day, 1, semana_hasta)
		from dbo.DTPO_LK_SEMANAS
		where id_semana = (
				select max(id_semana) 
				from dbo.DTPO_LK_SEMANAS
				where id_mes = (@anio - 1) * 100 + 12 )

		set @nro_semana = 1
		set @nro_mes = 1
		set @semana_mes = 1
		set @corte = 0
		set @fec_hasta_anio = cast('31-12-' + convert(varchar, @anio) as datetime)
		set @desde = @fecha_ref
		set @hasta = DateAdd(day, 6, @desde)

		insert into @tblSemanas (anio, nro_semana, fec_desde, 
			fec_hasta, nro_mes, semana_mes)
		values (@anio, @nro_semana, @desde, 
			@hasta, @nro_mes, @semana_mes)

		while @corte = 0
			Begin
				set @desde = DateAdd(day, 1, @hasta)
				set @hasta = DateAdd(day, 7, @hasta)
				set @fec_corte = DateAdd(day, 4, @desde)

				set @nro_semana = @nro_semana + 1
				set @nro_mes = month(@fec_corte)
				set @semana_mes = @semana_mes + 1

				insert into @tblSemanas (anio, nro_semana, fec_desde, 
					fec_hasta, nro_mes, semana_mes)
				values (@anio, @nro_semana, @desde, 
					@hasta, @nro_mes, @semana_mes)

				-- Cambio de Mes
				if month(@fec_corte) <> month(DateAdd(day, 7, @fec_corte))
					set @semana_mes = 0

				-- Corte del Año
				if  DateAdd(day, 7, @fec_corte) > @fec_hasta_anio 
				begin
					set @corte = 1
				end
			End

		insert into dtpo_lk_semanas (
			id_semana, nro_semana_anio, desc_semana_anio,
			semana_desde, semana_hasta, nro_semana_mes,
			id_semana_ant, id_semana_anio_ant,
			id_mes, desc_semana_mes)
		select id_semana, nro_semana, ref_sem +'-'+ convert(varchar, anio) as descrip,
			x.fec_desde, x.fec_hasta, x.semana_mes, 
			semana_anio_ant as sem_ant, 
			cast(convert(varchar, anio - 1) + ref_sem as numeric) as sem_anio_ant,
			cast(convert(varchar, anio) + ref_mes as numeric) as id_mes,
			ref_sem_mes +'-'+ substring(etl.fn_getDescMes(nro_mes),1,3) as desc_semana_mes
		from (
			select cast(convert(varchar, ((anio * 100) + nro_semana)) as numeric) as id_semana , 
				substring(convert(varchar, nro_semana + 100),2,2) as ref_sem,
				substring(convert(varchar, semana_mes + 100),2,2) as ref_sem_mes,
				substring(convert(varchar, nro_mes + 100),2,2) as ref_mes,
				case when nro_semana <> 1 then
						convert(varchar, anio) + substring(convert(varchar, (nro_semana-1)+100),2,2)
					else
						convert(varchar, anio - 1) + convert(varchar,@ult_nro_sem_anio_ant)
						end as semana_anio_ant,
				a.nro_semana, a.fec_desde, a.fec_hasta, a.nro_mes, a.semana_mes, a.anio
			from @tblSemanas a) as x
		order by 1
		set @msg = 'Semanas Insertadas: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		-- DIA 
		---------------------------
		declare @tblDias table (
			anio int,
			nro_dia int,
			fecha datetime
		)

		set @desde=cast('01-01-' + convert(varchar, @anio ) as datetime) 
		set @hasta=cast('31-12-' + convert(varchar, @anio ) as datetime)

		-- Procesar 
		set @fecha = @desde
		while @fecha <= @hasta
			begin
				insert into @tblDias (anio, nro_dia, fecha)
				values (year(@fecha), day(@fecha), @fecha)
			
				-- next dia
				set @fecha = DateAdd(day, 1, @fecha)
			end

		insert into dtpo_lk_dias (
			id_dia, fecha, id_dia_semana, id_semana, id_mes)
		select (a.anio * 10000) + ( month(a.fecha) * 100) + a.nro_dia as id_dia, 
			fecha, datepart(weekDay, a.fecha), b.id_semana, (a.anio * 100) + month(a.fecha) as id_mes
		from @tblDias a, dtpo_lk_semanas b
		where a.fecha between semana_desde and semana_hasta
		order by 1
		set @msg = 'Días Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		commit transaction

	end try
	
	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'PROCESO', @idBatch
	end catch
	-- exec etl.carga_lks_tiempo_anual 0, 2025

end
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_df]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_df]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_ctrl_verificacion_df] (
	@idProceso numeric(15),
	@fechaProceso datetime,
	@run bit,
	@runVerif bit,
	@estadoProceso char(2)
	)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 21-04-2014
	-- Description:	Controla la verificacion de Datos Fuentes de un Proceso luego de su posible
	--				ejecucion en el contexto de la Secuencia que lo dispara....
	-- ==========================================================================================
	-- 21/04/2014 - Version Inicial
	-- 14/05/2014 - LS - Modif. en Procesamientos de Datos (Se Agrega Horas Facturadas)
	-- 11/08/2014 - LS - Agrega Nuevas Tablas para saber si Hubo procesamiento.
	-- 14/08/2014 - LS - Quita STG Nomina y Rotacion
	-- 19/08/2014 - LS - Mejora en Obtencion Registros de STG
	-- 06/10/2014 - LS - Mejora Obtencion en Nombre Tabla STG (Casos LKs)

	declare @verifActiva char(1), @regProceso numeric(10), 
		@activacion bit, @desActivacion bit, @tbProceso varchar(128),
		@sql varchar(500), @msg varchar(500), @nmProceso varchar(128)
	declare @tbRegistro table (cantidad numeric(15))


	begin try
		
		-- Si no requiere verificacion, no hay control
		if exists (
			select 1
			from etl.prc_proceso
			where id_proceso = @idProceso
				and verificacion_df <> 'S')
			return 0

		set @activacion = 0
		set @desActivacion = 0
			
		-- Verificacion Activa
		select @verifActiva = verif_activa
		from etl.prc_df_verificacion
		where id_proceso = @idProceso
			and @fechaProceso between fecha_desde and fecha_hasta

		-- Verificar si hubo procesamiento de datos / Carga de Datos ??????
		-- Esto Deberia CAmbiar y Dicho Valor se deberia tomar de Otro Lado
		-- Si hay Registros en la STG es indicativo que se obtuvieron datos de DF, ya
		-- que en la STG se realiza la bajada.....
		select @nmProceso = proceso 
		from etl.prc_proceso
		where id_proceso = @idProceso

		if not exists(select 1 
				from sys.tables
				where name like 'ST_%' + @nmProceso
					and schema_name( schema_id ) = 'stg')
			begin
				set @msg = 'Tabla Stage Inexistente para el control de VDF. Tabla: ' + isnull(@nmProceso,'<>')
				exec etl.prc_log_error_seq @msg
				set @regProceso = 0
			end
		else
			begin
				-- Obtener el Nombre de Tabla
			 	select @tbProceso = name 
				from sys.tables
				where name like 'ST_%' + @nmProceso
					and schema_name( schema_id ) = 'stg'	

				set @sql =  'select count(1) as cantidad from stg.' +@tbProceso +' where fecha_carga = convert(varchar, getdate(), 103)'
				insert @tbRegistro exec (@sql)
				select @regProceso = cantidad from @tbRegistro
			end

		-- Si el Estado de Ejecucion del proceso es Error o No Ejecucion
		-- por ahora no hago nada....
		if @estadoProceso = 'ER' or @estadoProceso = 'NO'
			goto fin

		-- Si el Proceso Corrió...............
		if @run = 1
			begin 
				-- Activa Verificacion
				if @verifActiva = 'N' and @regProceso = 0 
					begin
						set @activacion = 1
						goto fin
					end

				-- Corrida Verif (Aqui la Verif. esta Activa)
				if @runVerif = 1
					if @estadoProceso in('OK','WN') and @regProceso > 0
						begin
							set @desActivacion = 1
							goto fin
						end
					
			end 	
					
		-- Finaliza Verificacion
		fin:
			
		begin tran

		if @desActivacion = 1 or ( @verifActiva = 'S' 
				and etl.fn_validar_corrida(@idProceso, dateadd(day, 1 , @fechaProceso)) = 1 )
			update etl.prc_df_verificacion
			set verif_activa = 'N',
				final_verificacion = etl.fn_truncFecha( getdate() )
			where id_proceso = @idProceso
				and @fechaProceso between fecha_desde and fecha_hasta
		else
			if @activacion = 1
				update etl.prc_df_verificacion
				set verif_activa = 'S'
				where id_proceso = @idProceso
					and @fechaProceso between fecha_desde and fecha_hasta

		commit tran

		return 1

	end try
	begin catch
		if xact_state() <> 0
			rollback tran

		return -1
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_job]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_ctrl_verificacion_job]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_ctrl_verificacion_job] (
   @JobName NVARCHAR(4000) )
as 
begin
 
	/*

	Es necesario en ciertas ocasiones, tener que auditar ciertos jobs sabiendo que su ejecución es crucial y 
	no debe demorarse mas de cierto tiempo.
	Para estos casos, si el mismo se encuentra ejecutando alguna rutina rara, o esta lockeado por otro proceso y 
	nosotros no estamos chequeando los mismos por estar fuera de la oficina, es necesario contar con un control 
	en backgroud que nos ayude a mantener las cosas en orden.

	Este Stored Procedure, generado en la base Master por cuestiones de comodidad, sirve para determinar si todo 
	esta Ok o no. Si el mismo devuelve 1, esta todo dentro del tiempo estipulado en las siguientes variables:

	@Minutes_FROM_LAST_DURATION = 40
	@Minutes_FROM_LAST_START = 90

	Es decir, dicho proceso no puede exceder su ejecución en mas de 40 minutos, y ademas, no puede pasar 
	mas 1 hora 30 minutos desde su ultima ejecución exitosa.
	En caso de que dicho Job finalice con error, dispondremos de otro sensor que audite dicho estado para 
	su posterior informe.

	*/

	DECLARE @Minutes_FROM_LAST_START SMALLINT 
	DECLARE @Minutes_FROM_LAST_DURATION SMALLINT 
	DECLARE @LastRunTime DATETIME  
 
	DECLARE      @StartTime AS DATETIME
	DECLARE      @Duration AS DATETIME
	DECLARE      @Duration_MINUTES AS INT
	DECLARE      @EndExecution AS DATETIME
 
	----------------------------------------
	SET @Minutes_FROM_LAST_START = 90  
	SET @Minutes_FROM_LAST_DURATION = 40
	----------------------------------------



 
	SELECT  @StartTime =    CAST(CONVERT(CHAR(10), CAST(STR(JH.run_date,8, 0) AS DATETIME), 112) + ' ' +   
							STUFF(STUFF(RIGHT('000000' + CAST(JH.run_time AS VARCHAR(6)) ,6),5,0,':'),3,0,':') AS DATETIME),
			@Duration  =    CAST(STUFF(STUFF(RIGHT('000000' + CAST(JH.run_duration AS VARCHAR(6)),6),5,0,':'),3,0,':') AS DATETIME),
			@EndExecution = CAST(CONVERT(CHAR(10), CAST(STR(JH.run_date,8, 0) AS DATETIME), 112) + ' ' +   
							STUFF(STUFF(RIGHT('000000' + CAST(JH.run_time AS VARCHAR(6)) ,6),5,0,':'),3,0,':') AS DATETIME) +  
							CAST(STUFF(STUFF(RIGHT('000000' + CAST(JH.run_duration AS VARCHAR(6)),6),5,0,':'),3,0,':') AS DATETIME)
	FROM msdb.dbo.sysjobs J  
	INNER JOIN msdb.dbo.sysjobhistory JH  
	ON  J.job_id = JH.job_id  
	WHERE J.name = @JobName  
	AND  Jh.step_id = 0  
	AND  JH.run_status = 1   
 
	/*
	SELECT @StartTime AS StartTime,
				 @Duration AS Duration,
				 @EndExecution AS EndExecution
 
   
	SELECT DATEDIFF(MINUTE, @EndExecution, GETDATE()) AS Diferencia  
	SELECT @EndExecution AS EndExecution  
	SELECT GETDATE()  
	*/
 
	SELECT @Duration_MINUTES = DATEPART(HOUR,@Duration) * 60 + DATEPART(MINUTE,@Duration)
   
	IF DATEDIFF(MINUTE, @EndExecution, GETDATE()) > @Minutes_FROM_LAST_START  OR @Duration_MINUTES>@Minutes_FROM_LAST_DURATION
	BEGIN 
	 SELECT -1 as Rtn, 'Proceso Demorado en mas de 1 hora...' as Resultado
	END ELSE 
	BEGIN 
	 SELECT 1  as Rtn, 'Proceso en Tiempo OK !!!' as Resultado
	END 
end
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_control]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_control]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_enviar_mail_control] (
	@fechaProceso datetime)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 09-04-2014
	-- Description:	Enviar Mail de Control de Datos / Errores
	-- ==========================================================================================
	-- 09/04/2014 - Version Inicial
	-- 21/05/2014 - LS - Agrega Controles Varios 
	-- 03/06/2014 - LS - No Cuenta ISK Verificados y Toma los de la Fecha
	-- 24/07/2014 - LS - Se Agrega Tipo de Control CVR (Control Varios)
	-- 21/10/2015 - Agregar Ambiente y Lista de Envios

	set nocount on

	begin try 

		declare @htm varchar(max),
			@asunto varchar(255),
			@tbCtrl varchar(max),
			@idCtrl varchar(10), 
			@fcCtrl varchar(30), 
			@descripcion varchar(255), 
			@proceso varchar(100),
			@cantISK varchar(10),
			@tipoError varchar(30),
			@tipoCtrl varchar(30),
			@lst_distr varchar(500)

		set @fechaProceso = etl.fn_TruncFecha( @fechaProceso )
		set @lst_distr = etl.fn_getParametroGral('LST-MAIL-PRC-CONTROL')

		-- Controles
		select a.*, b.estado as prc_estado, b.mensaje as prc_mensaje,
			c.id_proceso, c.proceso
		into #infoCtrl
		from etl.prc_ctrl_datos a
			left outer join etl.prc_proceso_batch b on a.id_batch = b.id_batch
				left outer join etl.prc_proceso c on c.id_proceso = b.id_proceso
		where etl.fn_TruncFecha( fecha_control ) = @fechaProceso

		----------
		-- Header
		----------
		set @htm = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">'
		set @htm = @htm + '<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="es" xml:lang="es">'
		set @htm = @htm + '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8" />'
		set @htm = @htm + '<title>Control de Procesos - ETLs Modelo Nuevo</title>'
		set @htm = @htm + '<style type="text/css">'
		set @htm = @htm + 'table.tablePrc {	font-family: verdana,arial,sans-serif; 	font-size:12px; color:#333333; 	border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.tablePrc th { background:#b5cfd2 url(''cell-blue.jpg''); border-width: 1px; padding: 8px; border-style: solid; border-color: #330099; }'
		set @htm = @htm + 'table.tablePrc td { background:#dcddc0 url(''cell-grey.jpg''); border-width: 1px; padding: 8px;	border-style: solid; border-color: #999999; }'
		set @htm = @htm + 'table.detalle {font-family: verdana,arial,sans-serif; font-size:9px; color:#333333; border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.detalle th {background: #003399; border-width: 1px; padding: 6px; border-style: solid; border-color: #CC0000; color: white;}'
		set @htm = @htm + 'table.detalle td {background: #d4e3e5; border-width: 1px; padding: 6px; border-style: solid; border-color: #999999;}'
		set @htm = @htm + '</style>'
		set @htm = @htm + '</head><body>'

		----------
		-- Body
		----------

		-- Tipo de Control: MAP
		--------------------------
		set @tbCtrl = ''

		declare lc_cursor cursor for	
		select substring(cast(id_control + 100000000 as varchar), 2, 8) id_ctrl,
			convert(varchar, fecha_control, 20) as fc_ctrl, 
			descripcion, 
			isnull('(' + cast(id_proceso as varchar) + ') ' + proceso + ' - BATCH: ' + substring(convert(varchar(10), id_batch + 100000000),2,8), '<Sin Batch>') as proceso
		from #infoCtrl where tipo_control = 'MAP'
		order by fecha_control

		open lc_cursor
		fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @proceso
		while  @@fetch_status = 0
			begin

				set @tbCtrl = @tbCtrl + '<tr>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@idCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@fcCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@descripcion+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@proceso+'</td>'
				set @tbCtrl = @tbCtrl + '</tr>'

				--next 
				fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @proceso
			end
		close lc_cursor
		deallocate lc_cursor

		set @htm = @htm + '<table class="tablePrc" width="90%">'
		set @htm = @htm + '<tr><th><u>CONTROL</u>:&nbsp;<span style="color: olive;"><b>MAPPING</b></span></th></tr>'
		set @htm = @htm + '</table>'
		set @htm = @htm + '<table class="detalle" width="90%">'
		set @htm = @htm + '<tr><th>CONTROL</th><th>FECHA</th><th>DETALLE</th><th>PROCESO</th></tr>'
		set @htm = @htm + @tbCtrl
		set @htm = @htm + '</table>'
		set @htm = @htm + '<hr clear width="90%" align="left"><br>'

		
		
		-- Tipo de Control: ISK
		--------------------------
		set @tbCtrl = ''

		declare lc_cursor cursor for	
		select substring(cast(a.id_control + 100000000 as varchar), 2, 8) id_ctrl,
			convert(varchar, a.fecha_control, 20) as fc_ctrl, 
			a.descripcion,  substring(cast(isnull(b.cant_sk, 0) + 1000000  as varchar),2,6) as cant_isk, 
			isnull('(' + cast(a.id_proceso as varchar) + ') ' + a.proceso + ' - BATCH: ' + substring(convert(varchar(10), a.id_batch + 100000000),2,8), '<Sin Batch>') as proceso
		from #infoCtrl a
			left outer join (select id_control, sum(cantidad) as cant_sk
								from etl.prc_ctrl_sks x
								where verif = 'N'
									and etl.fn_truncFecha (fecha_registro) = @fechaProceso
								group by id_control) b
							on a.id_control = b.id_control
		where tipo_control = 'ISK'
		order by a.id_control

		open lc_cursor
		fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @cantISK, @proceso
		while  @@fetch_status = 0
			begin

				set @tbCtrl = @tbCtrl + '<tr>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@idCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@fcCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@descripcion+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@cantISK+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@proceso+'</td>'
				set @tbCtrl = @tbCtrl + '</tr>'

				--next 
				fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @cantISK, @proceso
			end
		close lc_cursor
		deallocate lc_cursor

		set @htm = @htm + '<table class="tablePrc" width="90%">'
		set @htm = @htm + '<tr><th><u>CONTROL</u>:&nbsp;<span style="color: olive;"><b>INCONSISTENCIAS SK (ISK)</b></span></th></tr>'
		set @htm = @htm + '</table>'
		set @htm = @htm + '<table class="detalle" width="90%">'
		set @htm = @htm + '<tr><th>CONTROL</th><th>FECHA</th><th>DETALLE</th><th>Q -> ISKs</th><th>PROCESO</th></tr>'
		set @htm = @htm + @tbCtrl
		set @htm = @htm + '</table>'
		set @htm = @htm + '<hr clear width="90%" align="left"><br>'


		-- Tipo de Control: VARIOS
		--------------------------
		set @tbCtrl = ''

		declare lc_cursor cursor for	
		select substring(cast(id_control + 100000000 as varchar), 2, 8) id_ctrl,
			convert(varchar, fecha_control, 20) as fc_ctrl, 
			descripcion, 
			tipo_control,
			isnull('(' + cast(id_proceso as varchar) + ') ' + proceso + ' - BATCH: ' + substring(convert(varchar(10), id_batch + 100000000),2,8), '<Sin Batch>') as proceso
		from #infoCtrl where tipo_control in ('CDF','CVR')
		order by id_control

		open lc_cursor
		fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @tipoCtrl, @proceso
		while  @@fetch_status = 0
			begin

				set @tbCtrl = @tbCtrl + '<tr>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@idCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@fcCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@descripcion+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@tipoCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@proceso+'</td>'
				set @tbCtrl = @tbCtrl + '</tr>'

				--next 
				fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @tipoCtrl, @proceso
			end
		close lc_cursor
		deallocate lc_cursor

		set @htm = @htm + '<table class="tablePrc" width="90%">'
		set @htm = @htm + '<tr><th><u>CONTROL</u>:&nbsp;<span style="color: purple;"><b>VARIOS</b></span></th></tr>'
		set @htm = @htm + '</table>'
		set @htm = @htm + '<table class="detalle" width="90%">'
		set @htm = @htm + '<tr><th>CONTROL</th><th>FECHA</th><th>DETALLE</th><th>TIPO</th><th>PROCESO</th></tr>'
		set @htm = @htm + @tbCtrl
		set @htm = @htm + '</table>'
		set @htm = @htm + '<hr clear width="90%" align="left"><br>'


		-- Tipo de Control: ERRORES
		--------------------------
		set @tbCtrl = ''

		declare lc_cursor cursor for	
		select substring(cast(a.id_error + 100000000 as varchar), 2, 8) as id_error, 
			convert(varchar, a.fecha_error, 20) fc_error,
			a.mensaje_error as descripcion, a.tipo_error, 
			isnull('(' + cast(c.id_proceso as varchar) + ') ' + c.proceso + ' - BATCH: ' + substring(convert(varchar(10), a.id_batch + 100000000),2,8), '<Sin Batch>') as proceso
		from etl.prc_errores a
			left outer join etl.prc_proceso_batch b on a.id_batch = b.id_batch
				left outer join etl.prc_proceso c on c.id_proceso = b.id_proceso
		where 1=1
			and etl.fn_TruncFecha( fecha_error ) = @fechaProceso
		order by a.id_error	

		open lc_cursor
		fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @tipoError, @proceso
		while  @@fetch_status = 0
			begin

				set @tbCtrl = @tbCtrl + '<tr>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@idCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@fcCtrl+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@descripcion+'</td>'
				set @tbCtrl = @tbCtrl + '<td align="center">'+@tipoError+'</td>'
				set @tbCtrl = @tbCtrl + '<td>'+@proceso+'</td>'
				set @tbCtrl = @tbCtrl + '</tr>'

				--next 
				fetch next from lc_cursor into @idCtrl, @fcCtrl, @descripcion, @tipoError, @proceso
			end
		close lc_cursor
		deallocate lc_cursor

		set @htm = @htm + '<table class="tablePrc" width="90%">'
		set @htm = @htm + '<tr><th><u>CONTROL</u>:&nbsp;<span style="color: red;"><b>ERRORES GENERALES</b></span></th></tr>'
		set @htm = @htm + '</table>'
		set @htm = @htm + '<table class="detalle" width="90%">'
		set @htm = @htm + '<tr><th>ERROR</th><th>FECHA</th><th>DETALLE</th><th>TIPO ERROR</th><th>PROCESO</th></tr>'
		set @htm = @htm + @tbCtrl
		set @htm = @htm + '</table>'
		set @htm = @htm + '<hr clear width="90%" align="left">'


		----------
		-- Foot
		----------
		set @htm = @htm + '<br></body></html>'

		--select len(@htm) as lon
		--select @htm as html
		--return

		select @asunto = etl.fn_getDBAmbiente() + ' - Control de Datos - ' + convert(varchar, @fechaProceso, 103)
		
		-- Enviar Mail
		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 
	end try

	begin catch
		
		set @htm = '<html><body>'
		set @htm = @htm + '<h1 style="color: blue">Ocurrió un Error al Enviar Mail de Control de Datos de Procesos</h1>'
		set @htm = @htm + '<br><span style="color: red"><b>** ERROR: '+  isnull(ERROR_MESSAGE(),'<>') + '</b></span>'
		set @htm = @htm + '</body></html>'

		select @asunto = etl.fn_getDBAmbiente() + ' - ** ERROR: Envio Mail de Control de Datos - ' + convert(varchar, @fechaProceso, 103)

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 
	end catch

	-- etl.prc_enviar_mail_control '2014-06-02'
end
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_operativo]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_operativo]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_enviar_mail_operativo]
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 13-05-2014
	-- Description:	Enviar Mail Operativo de Procesos ETLs
	-- ==========================================================================================
	-- 15/05/2014 - Version Inicial
	-- 15/10/2014 - LS - Agrega Secuencia Inicial
	-- 19/01/2015 - LS - Agrega a Horacio en Envio de Mail
	-- 06/02/2015 - LS - Sacar a Horacio en Envio de Mail
	-- 21/10/2015 - Agregar Ambiente y Lista de Envios

	set nocount on

	begin try 

		declare @fechaProceso datetime,
			@htm varchar(max),
			@asunto varchar(255),
			@idSecuencia numeric(10),
			@idSecuenciaAct numeric(10),
			@secuencia varchar(100),
			@orden numeric(5),
			@proceso varchar(60),
			@estado varchar(255),
			@mensaje varchar(500),
			@reproceso char(1),
			@batch numeric(15),
			@lst_distr varchar(500)

		set @fechaProceso = etl.fn_TruncFecha( getdate())
		set @lst_distr = etl.fn_getParametroGral('LST-MAIL-PRC-OPERATIVO')

		-- Procesos Diarios
		select *
		into #info
		from (
			select id_secuencia, a.seq_nombre, '[Job_Secuencia_Inicial]' Job,
				ejecucion,	prc_nombre + ' - (' + substring(cast(id_proceso + 100000 as varchar),2,5) + ')' as proceso,
				prc_estado,	prc_reproceso, prc_mensaje,
				prc_fecha_inicio as inicio, prc_fecha_final as final,	
				prc_tiempo, prc_id_batch
			from etl.fn_infoSecuencia(0, @fechaProceso) a
			union all
			select id_secuencia, a.seq_nombre, '[Job_Secuencia_Carga_Diaria]' Job,
				ejecucion,	prc_nombre + ' - (' + substring(cast(id_proceso + 100000 as varchar),2,5) + ')' as proceso,
				prc_estado,	prc_reproceso, prc_mensaje,
				prc_fecha_inicio as inicio, prc_fecha_final as final,	
				prc_tiempo, prc_id_batch
			from etl.fn_infoSecuencia(1, @fechaProceso) a
			union all
			select id_secuencia, a.seq_nombre, '[Job_Secuencia_Carga_FTs]' Job,
				ejecucion,	prc_nombre + ' - (' + substring(cast(id_proceso + 100000 as varchar),2,5) + ')' as proceso,
				prc_estado,	prc_reproceso, prc_mensaje,
				prc_fecha_inicio as inicio, prc_fecha_final as final,	
				prc_tiempo, prc_id_batch
			from etl.fn_infoSecuencia(2, @fechaProceso) a
		) x
		order by id_secuencia, ejecucion

		----------
		-- Header
		----------
		set @htm = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">'
		set @htm = @htm + '<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="es" xml:lang="es">'
		set @htm = @htm + '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8" />'
		set @htm = @htm + '<title>Secuencia de Procesos - ETLs Modelo Nuevo</title>'
		set @htm = @htm + '<style type="text/css">'
		set @htm = @htm + 'table.tableSeq {	font-family: verdana,arial,sans-serif; 	font-size:12px; color:#333333; 	border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.tableSeq th { background:#b5cfd2 url(''cell-blue.jpg''); border-width: 1px; padding: 8px; border-style: solid; border-color: #330099; color:red; }'
		set @htm = @htm + 'table.tableSeq td { background:#dcddc0 url(''cell-grey.jpg''); border-width: 1px; padding: 8px;	border-style: solid; border-color: #999999; }'
		set @htm = @htm + 'table.ProcesoSeq {font-family: verdana,arial,sans-serif; font-size:9px; color:#333333; border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.ProcesoSeq th {background: #003399; border-width: 1px; padding: 6px; border-style: solid; border-color: #CC0000; color: white;}'
		set @htm = @htm + 'table.ProcesoSeq td {background: #dcddc0; border-width: 1px; padding: 6px; border-style: solid; border-color: #999999;}'
		set @htm = @htm + '</style>'
		set @htm = @htm + '</head><body>'

		----------
		-- Body
		----------
		declare lc_cursor cursor for	
		select id_secuencia,
			seq_nombre + ' - ' +job as secuencia,
			ejecucion as orden,
			proceso,
			'<span style="color: ' +
					case prc_estado when 'OK' then 'green'
								when 'ER' then 'red'
								when 'NO' then 'olive'
								when 'WN' then 'orange'
								else 'black'
					end 
					+ ';">' + prc_estado + '</span></b>' as estado,
			'<span style="color: ' +
					case prc_estado when 'OK' then 'green'
								when 'ER' then 'red'
								when 'NO' then 'olive'
								when 'WN' then 'orange'
								else 'black'
					end 
					+ ';">' + prc_mensaje + '</span></b>' as mensaje,
			prc_reproceso as reproceso,
			isnull(prc_id_batch,0) as batch
		from #info a
		
		open lc_cursor
		fetch next from lc_cursor into @idSecuencia, @secuencia, @orden,
			@proceso, @estado, @mensaje, @reproceso, @batch
		while  @@fetch_status = 0
			begin
				if isnull(@idSecuenciaAct,-1) <> @idSecuencia
					begin
						if isnull(@idSecuenciaAct,-1) >= 0
							set @htm = @htm + '</table><br>'

						set @htm = @htm + '<table class="tableSeq" width="90%">'
						set @htm = @htm + '<th>'+ @secuencia +'</th>'
						set @htm = @htm + '</table>'

						set @htm = @htm + '<table class="ProcesoSeq" width="90%">'
						set @htm = @htm + '<tr>'
						set @htm = @htm + '<th width="4%"># EJ.</th>'
						set @htm = @htm + '<th width="30%">PROCESO</th>'
						set @htm = @htm + '<th width="6%" align="center">ESTADO</th>'
						set @htm = @htm + '<th width="4%" align="center">REPRC.</th>'
						set @htm = @htm + '<th width="50%">MENSAJE</th>'
						set @htm = @htm + '<th width="6%" align="center">BATCH</th>'
						set @htm = @htm + '</tr>'
					end

				set @htm = @htm + '<tr>'
				set @htm = @htm + '<td align="center">' + substring(cast(@orden + 1000 as varchar),2,3) + '</td>'
				set @htm = @htm + '<td>' + @proceso + '</td>'
				set @htm = @htm + '<td align="center">' + @estado + '</td>'
				set @htm = @htm + '<td align="center">' + @reproceso + '</td>'
				set @htm = @htm + '<td>' + @mensaje + '</td>'
				set @htm = @htm + '<td align="center">' + cast(@batch as varchar) + '</td>'
				set @htm = @htm + '</tr>'
				set @idSecuenciaAct = @idSecuencia

				--next 
				fetch next from lc_cursor into @idSecuencia, @secuencia, @orden,
			@proceso, @estado, @mensaje, @reproceso, @batch
				if @@fetch_status <> 0 
					set @htm = @htm + '</table>'
			end
		close lc_cursor
		deallocate lc_cursor

		----------
		-- Foot
		----------
		set @htm = @htm + '<br></body></html>'

		--select len(@htm) as lon
		--select @htm as html
		--return

		select @asunto = etl.fn_getDBAmbiente() + ' - Procesos Operativos DW  - ' + convert(varchar, @fechaProceso, 103)
		
		-- Enviar Mail
		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 

		select 'Correo Enviado' as rtn


	end try

	begin catch
		
		set @htm = '<html><body>'
		set @htm = @htm + '<h1 style="color: blue">Ocurrió un Error al Enviar Mail de Control de Datos de Procesos</h1>'
		set @htm = @htm + '<br><span style="color: red"><b>** ERROR: '+  isnull(ERROR_MESSAGE(),'<>') + '</b></span>'
		set @htm = @htm + '</body></html>'

		select @asunto = etl.fn_getDBAmbiente() + ' - ** ERROR: Envio Mail de Procesos Operativos DW - ' + convert(varchar, @fechaProceso, 103)

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 

		select 'Error al Generar Correo' as rtn

	end catch

	-- etl.prc_enviar_mail_operativo
end
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_secuencia]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_secuencia]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_enviar_mail_secuencia] (
	@idSecuencia numeric(15),
	@fechaProceso datetime,
	@reproceso bit = 0)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 27-03-2014
	-- Description:	Enviar Mail de Ejecucion de Secuencia
	-- =============================================
	-- 27/03/2014 - Version Inicial
	-- 11/04/2017 - LS - Considera los procesos involucrados en un reproceso (s/n).
	-- 21/04/2017 - LS - Agrega Si corrió por Run Verificador DF (s/n).
	-- 02/07/2017 - LS - Agrega Id Demanda de Proceso, si tiene.
	-- 21/10/2015 - Agregar Ambiente y Lista de Envios

	begin try 
		declare @htm varchar(max),
			@secuencia varchar(500),
			@proceso varchar(500),
			@batch varchar(500),
			@tiempo varchar(500),
			@mensaje varchar(500),
			@logging varchar(500),
			@idBatch numeric(15),
			@idBatchAct numeric(15),
			@asunto varchar(255),
			@lst_distr varchar(500)

		set @fechaProceso = etl.fn_TruncFecha( @fechaProceso )
		set @lst_distr = etl.fn_getParametroGral('LST-MAIL-PRC-SECUENCIA')

		select a.*,
			etl.fn_tiempo(b.fecha_inicio, b.fecha_final) prc_tiempo_real,
			c.id_linea, c.mensaje
		into #infoSecuencia
		from etl.fn_infoSecuencia(@idSecuencia, @fechaProceso) a,
			etl.prc_proceso_batch b,
			etl.prc_proceso_batch_log c
		where a.prc_id_batch = b.id_batch
			and b.id_batch = c.id_batch
			and (@reproceso = 0 or (@reproceso = 1 and a.seq_reproceso = 'S' and a.prc_reproceso = 'S'))
		order by a.ejecucion, c.id_batch, c.id_linea

		if @@rowcount = 0
			begin
				if @reproceso = 1
					raiserror('No Existen Reprocesos para la Fecha de Proceso Determinada.', 16, 1) 
				else
					raiserror('No existen Procesos para la Fecha de Proceso Determinada.', 16, 1) 
			end			

		select distinct 
			@secuencia = '<tr><th><u>SECUENCIA</u>:&nbsp;' + upper(seq_nombre) + ' (' + substring(cast(id_secuencia + 1000 as varchar),2,3) + ')</th></tr>',
			@batch = '<tr><td><u>FECHA PROCESO</u>:&nbsp;' + convert(varchar(10), fecha_proceso, 103) +
				'&nbsp;-&nbsp;<u>BATCH #SEQ.</u>:&nbsp;' + substring(convert(varchar(10), seq_id_batch + 100000000),2,8) 
				+ '&nbsp;-&nbsp;<u>ESTADO</u>:&nbsp;<b><span style="color: ' +
				case seq_estado when 'OK' then 'green'
							when 'ER' then 'red'
							when 'NO' then 'olive'
							when 'WN' then 'orange'
							else 'black'
				end 
				+ ';">' + seq_estado + '</span></b>' 
				+ '&nbsp;-&nbsp;<u>REPROCESO</u>:&nbsp;'+seq_reproceso+'</td></tr>',
			@tiempo = '<tr><td><u>EJECUCION</u>:&nbsp;' + replace(convert(varchar, seq_fecha_inicio, 20), '-', '/') + ' - ' +
				replace(convert(varchar, seq_fecha_final, 20), '-', '/') + 
				'&nbsp;-&nbsp;<u>TIEMPO</u>:&nbsp; ('+seq_tiempo+')</td></tr>',
			@mensaje = '<tr><td><u>MENSAJE</u>:&nbsp;' + seq_mensaje + '</td></tr>'
		from #infoSecuencia

		----------
		-- Header
		----------
		set @htm = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">'
		set @htm = @htm + '<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="es" xml:lang="es">'
		set @htm = @htm + '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8" />'
		set @htm = @htm + '<title>Secuencia de Procesos - ETLs Modelo Nuevo</title>'
		set @htm = @htm + '<style type="text/css">'
		set @htm = @htm + 'table.tableSeq {	font-family: verdana,arial,sans-serif; 	font-size:12px; color:#333333; 	border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.tableSeq th { background:#b5cfd2 url(''cell-blue.jpg''); border-width: 1px; padding: 8px; border-style: solid; border-color: #330099; }'
		set @htm = @htm + 'table.tableSeq td { background:#dcddc0 url(''cell-grey.jpg''); border-width: 1px; padding: 8px;	border-style: solid; border-color: #999999; }'
		set @htm = @htm + 'table.ProcesoSeq {font-family: verdana,arial,sans-serif; font-size:9px; color:#333333; border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.ProcesoSeq th {background: #003399; border-width: 1px; padding: 6px; border-style: solid; border-color: #CC0000; color: white;}'
		set @htm = @htm + 'table.ProcesoSeq td {background: #d4e3e5; border-width: 1px; padding: 6px; border-style: solid; border-color: #999999;}'
		set @htm = @htm + '</style>'
		set @htm = @htm + '</head><body>'

		----------
		-- Body
		----------
		-- Secuencia
		set @htm = @htm + '<table class="tableSeq" width="70%">'
		set @htm = @htm + @secuencia
		set @htm = @htm + @batch
		set @htm = @htm + @tiempo
		set @htm = @htm + @mensaje
		set @htm = @htm + '</table>'
		set @htm = @htm + '<hr clear width="70%" align="left">'

		-- Procesos de la Secuencia
		declare lc_cursor cursor for	
		select prc_id_batch,
			'<tr><th>#:&nbsp;' + substring(cast(ejecucion + 100 as varchar),2,2) + '&nbsp;&nbsp;' +
			'<u>PROCESO</u>:&nbsp;' + prc_nombre + ' (' +substring(cast(id_proceso + 10000 as varchar),2,4) + ')' as proceso,

			'<tr><td><u>BATCH #PRC.</u>:&nbsp;' + substring(convert(varchar(10), prc_id_batch + 100000000),2,8) 
					+ '&nbsp;-&nbsp;<u>ESTADO</u>:&nbsp;<b><span style="color: ' +
					case prc_estado when 'OK' then 'green'
								when 'ER' then 'red'
								when 'NO' then 'olive'
								when 'WN' then 'orange'
								else 'black'
					end 
					+ ';">' + prc_estado + '</span></b>'
					+ '&nbsp;-&nbsp;<u>REPROCESO</u>:&nbsp;'+prc_reproceso
					+ '&nbsp;-&nbsp;<u>VERIF. DF</u>:&nbsp;' + 
					case prc_run_verif when 1 then 'S'
						else 'N' 
					end+ '&nbsp;-&nbsp;<u>DEMANDA</u>:&nbsp;' + 
					case when prc_id_demanda is null then '0' 
						else substring(convert(varchar(10), prc_id_demanda + 100000),2,5) 
					end+'</td></tr>' as batch,
			'<tr><td><u>EJECUCION</u>:&nbsp;' + replace(convert(varchar, prc_fecha_inicio, 20), '-', '/') + ' - ' +
					replace(convert(varchar, prc_fecha_final, 20), '-', '/') + 
					'&nbsp;-&nbsp;<u>TIEMPO</u>:&nbsp; ('+prc_tiempo+')</td></tr>' tiempo,
			'<tr><td><u>MENSAJE</u>:&nbsp;' + prc_mensaje + '</td></tr>' as mensaje,
			'<tr><td style="background: #CCFF99;"><u>'+substring(cast(id_linea + 100 as varchar),2,2)+'</u>:&nbsp;' + mensaje + '</td></tr>' as logging
		from #infoSecuencia
		order by ejecucion, id_linea

		open lc_cursor
		fetch next from lc_cursor into @idBatch, @proceso, @batch, @tiempo, @mensaje, @logging
		while  @@fetch_status = 0
			begin
				if isnull(@idBatchAct,0) <> @idBatch
					begin
						if isnull(@idBatchAct,0) > 0
							set @htm = @htm + '</table><br>'

						set @htm = @htm + '<table class="ProcesoSeq" width="70%">'
						set @htm = @htm + @proceso
						set @htm = @htm + @batch
						set @htm = @htm + @tiempo
						set @htm = @htm + @mensaje
					end
					set @htm = @htm + @logging
					set @idBatchAct = @idBatch

				--next 
				fetch next from lc_cursor into @idBatch, @proceso, @batch, @tiempo, @mensaje, @logging
				if @@fetch_status <> 0 
					set @htm = @htm + '</table>'
			end
		close lc_cursor
		deallocate lc_cursor
	
		----------
		-- Foot
		----------
		set @htm = @htm + '<br></body></html>'
	
		-- select len(@htm) as lon
		-- select @htm as html; 
		-- return

		select @asunto = etl.fn_getDBAmbiente() + ' - ' + asunto + reproceso
		from (select distinct '[' + substring(cast(id_secuencia + 1000 as varchar),2,3) + '] ' + seq_nombre + 
			' - ' + convert(varchar(10), fecha_proceso, 103) + ' - (ID: ' + substring(convert(varchar(10), seq_id_batch + 100000000),2,8) + ')' as asunto,
			 case seq_reproceso when 'S' then ' - REPROCESO' else '' end as reproceso
			from #infoSecuencia) x
		
		-- Enviar Mail
		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 
	end try

	begin catch
		set @htm = '<html><body>'
		set @htm = @htm + '<h1 style="color: blue">Ocurrió un Error al Enviar Mail de Secuencia Nro.: '+ cast(@idSecuencia as varchar) +'</h1>'
		set @htm = @htm + '<br><span style="color: red"><b>** ERROR: '+  isnull(ERROR_MESSAGE(),'<>') + '</b></span>'
		set @htm = @htm + '</body></html>'

		select @asunto = etl.fn_getDBAmbiente() + ' - ** Error al Enviar Mail de Secuencia Nro.: ' + cast(@idSecuencia as varchar) + ' - ' + convert(varchar, @fechaProceso, 103)

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 

	end catch
	-- etl.prc_enviar_mail_secuencia 2, '2014-07-02', default
end
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_usuario]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_enviar_mail_usuario]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_enviar_mail_usuario] (
	@idBatch numeric(15),
	@titulo varchar(500),
	@to varchar(500) = null,
	@cc varchar(500) = null,
	@colspan numeric(2) = 1)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 30/09/2015
	-- Description:	Enviar Mail a Usuarios con Info/Alerts
	-- ==========================================================================================
	-- 01/10/2015 - Version Inicial
	-- 21/10/2015 - Agregar Ambiente y Lista de Envios
	
	set nocount on

	begin try 

		declare @idBatchAct numeric(15),
			@htm varchar(max),
			@asunto varchar(255),
			@descripcion varchar(255), 
			@idLinea numeric(15),
			@fechaProceso datetime,
			@proceso varchar(500),
			@mensaje varchar(500),
			@logging varchar(500),
			@lst_distr varchar(500)


		set @fechaProceso = etl.fn_TruncFecha( getDate() )
		set @lst_distr = etl.fn_getParametroGral('LST-MAIL-PRC-USUARIO')

		if not exists (select 1 from etl.prc_mail_usuario_info
				where id_batch = @idBatch)
			return
			
		set @titulo = isnull(@titulo, 'Informacion de Procesamiento')

		-- drop table #info

		select a.id_proceso, a.proceso, c.id_batch as prc_id_batch,
			b.estado as prc_estado, 
			b.fecha_inicio as prc_fecha_inicio,
			b.fecha_final as prc_fecha_final,	
			etl.fn_tiempo( b.fecha_inicio, b.fecha_final) as prc_tiempo,
			etl.fn_tiempo(b.fecha_inicio, b.fecha_final) prc_tiempo_real,
			b.mensaje as prc_mensaje,
			c.id_linea, c.mensaje
		into #info
		from etl.prc_mail_usuario_info c
			left outer join etl.prc_proceso_batch b
				on b.id_batch = c.id_batch
					left outer join etl.prc_proceso a
						on a.id_proceso = b.id_proceso
		where c.id_batch = @idBatch

		if @@rowcount = 0
			return	


		----------
		-- Header
		----------
		set @htm = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">'
		set @htm = @htm + '<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="es" xml:lang="es">'
		set @htm = @htm + '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8" />'
		set @htm = @htm + '<title>'+@titulo+'</title>'
		set @htm = @htm + '<style type="text/css">'
		set @htm = @htm + 'table.tableSeq {	font-family: verdana,arial,sans-serif; 	font-size:12px; color:#333333; 	border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.tableSeq th { background:#b5cfd2 url(''cell-blue.jpg''); border-width: 1px; padding: 8px; border-style: solid; border-color: #330099; }'
		set @htm = @htm + 'table.tableSeq td { background:#dcddc0 url(''cell-grey.jpg''); border-width: 1px; padding: 8px;	border-style: solid; border-color: #999999; }'
		set @htm = @htm + 'table.ProcesoSeq {font-family: verdana,arial,sans-serif; font-size:9px; color:#333333; border-width: 1px; border-color: #999999; border-collapse: collapse; }'
		set @htm = @htm + 'table.ProcesoSeq th {background: #003399; border-width: 1px; padding: 6px; border-style: solid; border-color: #CC0000; color: white;}'
		set @htm = @htm + 'table.ProcesoSeq td {background: #d4e3e5; border-width: 1px; padding: 6px; border-style: solid; border-color: #999999;}'
		set @htm = @htm + '</style>'
		set @htm = @htm + '</head><body>'

		----------
		-- Body
		----------
		-- Info del Mail
		declare lc_cursor cursor for	
		select prc_id_batch,
			'<tr><th colspan="'+cast(@colspan as varchar(2))+'">&nbsp;&nbsp;<u>PROCESO</u>:&nbsp;' + isnull(proceso,'') + ' (' +substring(cast(isnull(id_proceso,0) + 10000 as varchar),2,4) + ')</th></tr>' as proceso,
			'<tr><td colspan="'+cast(@colspan as varchar(2))+'"><b><u>DETALLE</u>:&nbsp;<span style="color: red;">' + isnull(@titulo,'&nbsp;') + '</span></b></td></tr>' as mensaje,
			case when @colspan > 1 then 
				  '<tr>' + mensaje + '</tr>' 
				else
				  '<tr><td style="background: #CCFF99;">&nbsp;' + mensaje + '</td></tr>' 
				end as logging
		from #info
		order by id_linea

		open lc_cursor
		fetch next from lc_cursor into @idBatch, @proceso, @mensaje, @logging
		while  @@fetch_status = 0
			begin
				if @idBatchAct <> @idBatch or @idBatchAct is null
					begin
						set @htm = @htm + '<table class="ProcesoSeq">'
						set @htm = @htm + @proceso
						set @htm = @htm + @mensaje
						set @idBatchAct = @idBatch
						if @htm is null
							set @htm = '<table class="ProcesoSeq" width="70%">'

					end
					-- Msg
					set @htm = @htm + @logging
				--next 
				fetch next from lc_cursor into @idBatch, @proceso,  @mensaje, @logging
				if @@fetch_status <> 0 
					set @htm = @htm + '</table>'
			end
		close lc_cursor
		deallocate lc_cursor		


		----------
		-- Foot
		----------
		set @htm = @htm + '<br></body></html>'

		--select len(@htm) as lon
		--select @htm as html
		--return

		select @asunto = etl.fn_getDBAmbiente() + ' - ' + @titulo + ' - ' + convert(varchar, @fechaProceso, 103)

		
		-- Enviar Mail
		exec msdb.dbo.sp_send_dbmail 
			@profile_name = 'ETL_Nuevo',
			@recipients = @to,
			@copy_recipients = @cc,
			@blind_copy_recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 
	end try

	begin catch
		
		set @htm = '<html><body>'
		set @htm = @htm + '<h1 style="color: blue">Ocurrió un Error al Enviar Mail de Usuario</h1>'
		set @htm = @htm + '<br><span style="color: red"><b>** ERROR: '+  isnull(ERROR_MESSAGE(),'<>') + '</b></span>'
		set @htm = @htm + '</body></html>'

		select @asunto = etl.fn_getDBAmbiente() + 
			'** ERROR: Envio Mail de Usuario - ' + @titulo + ' - ' + convert(varchar, @fechaProceso, 103)

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'ETL_Nuevo',
			@recipients = @lst_distr,
			@subject = @asunto,
			@body = @htm,
			@body_format = 'HTML' 
	end catch

	-- etl.prc_enviar_mail_usuario 0, 'Cotizacion TyC', default, default
end
GO
/****** Object:  StoredProcedure [etl].[prc_exportar_isk]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [etl].[prc_exportar_isk] (
		@proceso varchar(50),
		@tabla varchar(50),
		@query varchar(500),
		@archivo varchar(100))
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 06-04-2014
	-- Description:	Exportar Datos de tablas sobre SK a un Archivo
	-- ==========================================================================================
	-- 09/04/2014 - Version Inicial
	-- 24/04/2014 - LS - Agranda tamaño de Variables
	-- 03/06/2014 - LS - Cambio de Nombre + Agrega Parametro Proceso
	-- 21/12/2021 - LS - Add Load Dir (Mig Azure)

	begin try
		declare @comando varchar(4000),
			@columnas varchar(4000),
			@dirISK varchar(255),
			@rtn int		

		-- Header
		select  @columnas = coalesce(@columnas + ', ', 'select ') + '''' + upper(column_name) + ''' as ' + column_name
		from (
			select column_name COLLATE database_default as column_name, ordinal_position as orden
			from tempdb.INFORMATION_SCHEMA.COLUMNS 
			where table_name = @tabla
			union
			select column_name COLLATE database_default, ordinal_position as orden
			from INFORMATION_SCHEMA.COLUMNS 
			where table_name = @tabla) x
		order by orden

		set @comando = 'bcp "' + @columnas + '" queryout "header.log" -T -c -k'
		exec xp_cmdshell @comando , no_output

		-- Datos
		set @comando = 'bcp "' + @query + '" queryout "datos.log" -T -c -k'
		exec xp_cmdshell @comando , no_output
		
		-- Directorio
		set @dirISK = etl.fn_getDirectorioLog(0) + 'ISK\' + @proceso + '\'
		set @comando = 'mkdir ' + @dirISK
		exec xp_cmdshell  @comando , no_output

		-- Unificar
		set @archivo = @dirISK + @archivo
		set @comando = 'copy /b header.log + datos.log ' + @archivo
		exec @rtn = xp_cmdshell @comando , no_output

		-- Borrar
		exec xp_cmdshell 'del header.log' , no_output
		exec xp_cmdshell 'del datos.log' , no_output
		
		return @rtn
	end try
	/* 
		etl.prc_exportar_isk 'FT_VISITA', '##isk_rsm_ft_visita',
			'select * from ##isk_rsm_ft_visita order by id_stage', 'testing.log'
	*/

	begin catch
		print error_message()
		return -1
	end catch
end 
GO
/****** Object:  StoredProcedure [etl].[prc_finalizar_proceso]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_finalizar_proceso]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_finalizar_proceso] (
	@idBatch numeric(15),
	@estado char(2))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 14-03-2014
	-- Description:	Finalizar Proceso
	-- =============================================
	begin try

		declare @activo char(1),
			@parametros varchar(500),
			@msg varchar(255),
			@archivo varchar(255),
			@sql varchar(500),
			@Existe int
		
		create table #logArchivo (
			fecha varchar(30),
			log varchar(250)
		)

		begin transaction

		update etl.prc_proceso_batch 
		set estado = @estado,
			fecha_final = getdate(),
			mensaje = 'Proceso Finalizado.'
		where id_batch = @idBatch

		-- Obtener Log del Archivo Mapping
		set @archivo = etl.fn_getNombreArchivoLog( @idBatch, 'MAP' )

		exec xp_fileexist @archivo , @Existe OUT
		if @Existe = 1
			begin
				set @sql = 'bulk insert #logarchivo from ''' + @archivo + ''' with (fieldterminator = ''|'')'
				exec (@sql)

				insert into etl.prc_proceso_batch_log (
					id_batch, id_linea, fecha, mensaje)
				select @idBatch, row_number() over(order by fecha) + ln as linea  , fecha, mensaje
				from (
					select convert(datetime, replace(fecha, '"', ''), 13) as fecha,
						replace(log, '"', '') as mensaje,
						y.ln
					from #logarchivo a,
						(select max(id_linea) ln
						from etl.prc_proceso_batch_log
						where id_batch = @idBatch) y
				) x

				-- Ordernar
				update pr
				set id_linea = rownumber 
				from etl.prc_proceso_batch_log pr, 
					(select id_linea, row_number() over (order by fecha) as rownumber
					from etl.prc_proceso_batch_log where id_batch = @idBatch) ord
				where pr.id_linea = ord.id_linea
					and pr.id_batch = @idBatch

			end

		-- Log Final del Proceso
		set @msg = replicate('=', 60)
		exec etl.prc_log_batch @idBatch, @msg

		select @msg = 'Proceso Finalizado (' + estado + ': ' +  etl.fn_tiempo(fecha_inicio, fecha_final) + ')'
		from etl.prc_proceso_batch 
		where id_batch = @idBatch
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = replicate('=', 60)
		exec etl.prc_log_batch @idBatch, @msg

		commit transaction

		-- OK
		return 1

	end try
	begin catch
		if xact_state() <> 0
			rollback transaction

		PRINT '** ERROR AL FINALIZAR PROCESO: ' + isnull(ERROR_MESSAGE(),'')

		-- ERROR
		return -1
	end catch
	-- etl.prc_finalizar_proceso 2, 'OK'
end
GO
/****** Object:  StoredProcedure [etl].[prc_gen_batch_secuencia]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_gen_batch_secuencia]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_gen_batch_secuencia] (
	@idSecuencia numeric(15),
	@fechaProceso datetime,
	@msg varchar(500) output,
	@idBatchSeq numeric(15) output)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 05/06/2014
	-- Description:	Genera Logica de Ejecucion de Secuencia
	-- =============================================
	-- 30/06/2014 - Version Inicial
	-- 16/07/2014 - LS - Toma Parametro de FN si no se especifica en la demanda
	-- 13/08/2014 - LS - Cambia Prioridad al Tomar Parametro de Demanda si existe una en la misma fecha
	-- 19/08/2014 - LS - Activa Run Verif; ya que si esta Activa en un fecha de proceso es indpte. en la ejecucion de una demanda...
	-- 16/10/2015 - LS - Secuencia Automatizable
	-- 03/01/2019 - LS - Ajuste para Secuencias sin Procesos
	-- 17/10/2019 - LS - Ajuste para considerar multiples horas de ejecucion de un mismo proceso (INC7882100)

	set nocount on

	begin try

		declare @secuencia varchar(100), @reEjecucion char(1), 
			@estado char(2), @nro_ejecucion numeric(10), @id_proceso numeric(15), @nombre varchar(60), @orden_ejec numeric(2), 
			@nivel_ejec numeric(3), @parametro varchar(500), @run_verif numeric(1), @run_fecha numeric(1),
			@horas_ejecucion varchar(255), @hora varchar(5)

		-- Verificar Secuencia
		select @secuencia = nombre, 
			@reEjecucion = isnull(reejecucion,'N')
		from etl.prc_secuencia
		where id_secuencia = @idSecuencia

		if @@rowcount = 0 
			begin
				set @msg = 'La Secuencia Nro. '+ isnull(cast(@idSecuencia as varchar),'<>') +' no está definida.'
				return -1
			end

		-- Fecha de Proceso
		set @fechaProceso = etl.fn_truncFecha( @fechaProceso )

		if @fechaProceso > getdate()
			begin
				set @msg = 'No es posible corre la secuencia con una fecha mayor a la fecha actual.'
				return -1
			end

		-- Verificar existencia de Secuencia para la Fecha de Proceso (Reproceso)
		if exists (select 1
			from etl.prc_batch_secuencia
			where id_secuencia = @idSecuencia
				and fecha_proceso = @fechaProceso )
			begin
				select @idBatchSeq = b.id_batch_seq,
					@estado = b.estado
				from etl.prc_batch_secuencia b
				where b.id_secuencia = @idSecuencia
					and b.fecha_proceso = @fechaProceso 
	
				if @estado = 'OK' and @reEjecucion = 'N'
					begin
						if @idSecuencia not in (3,102,103)
							begin
								set @msg = 'No se Permite la Re-Ejecucion de la Secuencia.'
								return -1
							end
						else
							begin
								set @msg = 'Secuencia No se Ejecuta (Ya se ejecuto OK y No permite Re-Ejecucion).'
								return 0
							end
					end
				else	
					begin
						update etl.prc_batch_secuencia
						set estado = 'IN',
							reproceso = case when @idSecuencia in (3,102,103) then isnull(reproceso,'N') else 'S' end,
							fecha_inicio = getdate(),
							fecha_final = getdate()
						where id_secuencia = @idSecuencia
							and fecha_proceso = @fechaProceso
					
						update etl.prc_batch_secuencia_proceso
						set estado = case when @idSecuencia in (3,102) then isnull(estado,'IN') else 'IN' end,
							reproceso = case when @idSecuencia in (3,102) then isnull(reproceso,'N') else 'S' end,
							fecha_inicio = getdate(),
							fecha_final = getdate(),
							mensaje =  case when @idSecuencia in (3,102) then isnull(mensaje, 'Reproceso Inicializado.') else  'Reproceso Inicializado.' end
						where id_batch_seq = @idBatchSeq
							and estado <> 'OK'
					end

			end
		else
			-- Armar Logica de Batch de Secuencia de Procesos
			begin
				-- Cabecera
				insert into etl.prc_batch_secuencia (
					id_secuencia, fecha_proceso, estado, reproceso)
				values (@idSecuencia, @fechaProceso, 'IN', 'N')
	
				set @idBatchSeq = SCOPE_IDENTITY()

				-- Detalle Procesos
				declare lc_cur cursor for
				select a.id_proceso, b.proceso, a.orden, a.nivel, 
					etl.fn_getValorParametroSecuencia(@fechaProceso, ult_proceso_ok, ejec_parametro) as parametro,
					etl.fn_validar_verif_df(b.id_proceso, @fechaProceso) as run_verif,
					etl.fn_validar_corrida(b.id_proceso, @fechaProceso) as run_fecha,
					a.horas_ejecucion
				from etl.prc_secuencia_proceso a,
					etl.prc_proceso b
				where a.id_proceso = b.id_proceso
					and a.id_secuencia = @idSecuencia
					and b.activo = 'S'
					and a.activo = 'S'
					and ( etl.fn_validar_corrida(b.id_proceso, @fechaProceso) = 1 
						or etl.fn_validar_verif_df(b.id_proceso, @fechaProceso) = 1)
				order by nivel, orden

				open lc_cur
				fetch next from lc_cur into @id_proceso, @nombre, @orden_ejec, @nivel_ejec,
					@parametro, @run_verif, @run_fecha, @horas_ejecucion
				while @@fetch_status = 0
					begin
						if @horas_ejecucion is null
							begin 
								set @nro_ejecucion = 1
								insert into etl.prc_batch_secuencia_proceso (
									id_batch_seq, id_proceso, nombre, orden_ejec, nivel_ejec,
									estado, reproceso, parametro, mensaje, run_verif, run_fecha, 
									nro_ejecucion, hora_ejecucion)
								values (@idBatchSeq, @id_proceso, @nombre, @orden_ejec, @nivel_ejec,
									'IN', 'N', @parametro, 'Proceso Inicializado.', @run_verif, @run_fecha, 
									@nro_ejecucion, null)
							end
						else
							begin
								set @nro_ejecucion = 0
								declare lc_cur_he cursor for
								select valor as hora
								from etl.fn_getTablaLista (@horas_ejecucion) a

								open lc_cur_he
								fetch next from lc_cur_he into @hora
								while @@fetch_status = 0
									begin

										set @nro_ejecucion = @nro_ejecucion + 1
										insert into etl.prc_batch_secuencia_proceso (
											id_batch_seq, id_proceso, nombre, orden_ejec, nivel_ejec,
											estado, reproceso, parametro, mensaje, run_verif, run_fecha, 
											nro_ejecucion, hora_ejecucion)
										values (@idBatchSeq, @id_proceso, @nombre, @orden_ejec, @nivel_ejec,
											'IN', 'N', @parametro, 'Proceso Inicializado.', @run_verif, @run_fecha, 
											@nro_ejecucion, @hora)

										-- Next
										fetch next from lc_cur_he into @hora
									end
								close lc_cur_he
								deallocate lc_cur_he

							end

						-- Next
						fetch next from lc_cur into @id_proceso, @nombre, @orden_ejec, @nivel_ejec,
							@parametro, @run_verif, @run_fecha, @horas_ejecucion
					end
				close lc_cur
				deallocate lc_cur


			end

		-- Verificar Procesos Demanda (De la Misma Secuencia)
		insert into etl.prc_batch_secuencia_proceso (
			id_batch_seq, id_proceso, nombre, orden_ejec, nivel_ejec,
			estado, reproceso, parametro, mensaje, run_verif, run_fecha,
			id_demanda)
		select @idBatchSeq, a.id_proceso, b.proceso, isnull(c.orden_ejec,a.orden), a.nivel, 
			'IN', 'N' , isnull(c.parametro,
					etl.fn_getValorParametroSecuencia(@fechaProceso, 
						b.ult_proceso_ok, b.ejec_parametro) ),
			'Proceso Inicializado.' as mensaje,
			etl.fn_validar_verif_df(b.id_proceso, @fechaProceso) as run_verif,
			0 as run_fecha,
			c.id_demanda
		from etl.prc_secuencia_proceso a,
			etl.prc_proceso b,
			etl.prc_demanda_ejecucion c
		where a.id_proceso = b.id_proceso
			and a.id_proceso = c.id_proceso
			and a.id_secuencia = @idSecuencia
			and b.activo = 'S'
			and (a.activo = 'S' or isnull(a.demanda,'N') = 'S')
			and c.fecha_demanda = @fechaProceso
			and c.estado in ('IN', 'ER')
			and not exists (select 1 from etl.prc_batch_secuencia_proceso d
							where d.id_batch_seq = @idBatchSeq
								and d.id_proceso = a.id_proceso)

		-- Si Hay un Demanda en la misma fecha, y tiene prioridad de ejecucion, 
		-- debe ejecutarse bajo Demanda debido seguramente a la utilizacion de parametros 
		-- u otra cuestion...
		update BAT
		set id_demanda = a.id_demanda,
			parametro = isnull(isnull(a.parametro, etl.fn_getValorParametroSecuencia(@fechaProceso, 
						b.ult_proceso_ok, b.ejec_parametro)),
				BAT.parametro ),
			estado = 'IN',
			mensaje = 'Proceso Inicializado.', 
			fecha_inicio = getdate(),
			fecha_final = getdate(),
			reproceso = reproceso,
			run_verif = etl.fn_validar_verif_df(b.id_proceso, @fechaProceso),
			orden_ejec =  isnull(a.orden_ejec, BAT.orden_ejec)
		from etl.prc_batch_secuencia_proceso BAT,
			etl.prc_demanda_ejecucion a,
			etl.prc_proceso b
		where BAT.id_proceso = a.id_proceso
			and BAT.id_proceso = b.id_proceso
			and BAT.id_batch_seq = @idBatchSeq
			and a.fecha_demanda = @fechaProceso
			and a.estado in ('IN', 'ER')
			and (a.prioridad_ejec = 1 or (BAT.id_demanda is not null))

		-- Rechazo de Demanda (Si corre en la misma fecha y no tiene prioridad de ejecucion
		-- la demanza es rechazada....
		update DMD
		set estado = 'DR',
			fecha_estado = getdate(),
			detalles = 'El proceso se rechaza porque no tiene prioridad de ejecución en la Secuencia.'
		from etl.prc_demanda_ejecucion DMD
		where DMD.fecha_demanda = @fechaProceso
			and DMD.estado in ('IN', 'ER')
			and DMD.prioridad_ejec = 0
			and exists (select 1 from etl.prc_batch_secuencia_proceso d
							where d.id_batch_seq = @idBatchSeq
								and d.id_proceso = DMD.id_proceso)

		-- No Hay Procesos
		if not exists (select 1 from etl.prc_batch_secuencia_proceso where id_batch_seq = @idBatchSeq and estado <> 'OK')
			begin
				update etl.prc_batch_secuencia
				set estado = 'OK',
					mensaje = isnull(mensaje,'No existen procesos a reprocesar')
				where id_batch_seq = @idBatchSeq

				set @msg = 'Secuencia No se Ejecuta (No existen procesos a reprocesar).'
				return 0
			end

		set @msg = 'OK'
		return 1

	end try

	-- Captura de Error
	begin catch
		set @msg = '** ERROR: "' + isnull(ERROR_MESSAGE(),'')
		exec etl.prc_log_error_seq @msg	

	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_gen_verificacion_df]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_gen_verificacion_df]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_gen_verificacion_df] (
	@fechaProceso datetime)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 16-04-2014
	-- Description:	Generar Periodos de Verificacion de Datos Fuentes
	--				Se va generando en forma Menual....
	-- ==========================================================================================
	-- 16/04/2014 - Version Inicial
	-- 15/05/2014 - Se Quito datos de periodo harcodeado por prueba....
	-- 02/06/2014 - Corrige Actualizacion de Fecha Hasta Mes Anterior + Mejoras...
	-- 16/10/2015 - LS - Ajuste Compatibilidad SQL Server 2012

	set nocount on

	declare @cantDias int, @mes int, @anio int
	declare @idProceso numeric(15), @proceso varchar(60), 
		@fecha datetime, @tipoPeriodo char(1)

	create table #verificacion (
		id_proceso numeric(15),
		tipo_periodo char(1) COLLATE database_default,
		proceso varchar(60) COLLATE database_default,
		fecha_desde datetime,
		fecha_hasta datetime)

	begin try

		-- Inicializar
		set @mes = month( @fechaProceso )
		set @anio = year( @fechaProceso )
		set @cantDias = day(etl.fn_getUltimoDiaMes ( @mes, @anio))

		-- Inicializa Tabla
		insert into #verificacion (id_proceso, proceso, 
			fecha_desde, fecha_hasta, tipo_periodo)
		select a.id_proceso,
			a.proceso,
			b.fecha,
			etl.fn_getUltimoDiaMes (@mes, @anio) fecha_hasta,
			a.tipo_periodo
		from dbo.ODS_DF_REGISTRO a,
			(	select convert(datetime, cast(nro as varchar) + '/' + cast(@mes as varchar) + '/' + cast(@anio as varchar), 103) as fecha
				from etl.fn_genTabla( 1, @cantDias, 1) ) b
		where id_registro = (select max(id_registro)
						from dbo.ODS_DF_REGISTRO b
						where a.id_proceso = b.id_proceso)
			and etl.fn_validar_corrida(id_proceso, fecha) = 1
		order by a.id_proceso, b.fecha

		begin transaction
		
		declare lc_cursor cursor for
		select id_proceso, fecha_desde, tipo_periodo 
		from #verificacion
		order by 1, 2, 3
		open lc_cursor
		fetch next from lc_cursor into @idProceso, @fecha, @tipoPeriodo
		while @@fetch_status = 0
			begin
				update #verificacion
				set fecha_hasta = isnull((select dateadd(day, -1, min(fecha_desde))
							from #verificacion b 
							where id_proceso = @idProceso
								and tipo_periodo = @tipoPeriodo
								and fecha_desde > @fecha), fecha_hasta)
				where id_proceso = @idProceso
					and tipo_periodo = @tipoPeriodo
					and fecha_desde = @fecha

				-- next
				fetch next from lc_cursor into @idProceso, @fecha, @tipoPeriodo
			end
		close lc_cursor
		deallocate lc_cursor


		-- Actualizar Fecha Hasta (Mes Anterior) con 
		update DF
		set  fecha_hasta = base.fecha_hasta_mes_ant
		from etl.prc_df_verificacion DF,
			(select id_proceso, tipo_periodo, 
				dateadd(day, -1, min(fecha_desde)) as fecha_hasta_mes_ant,
				month(dateadd(month, -1, min(fecha_desde))) as mes_ref
			from #verificacion
			group by id_proceso, tipo_periodo) base
		where DF.id_proceso = base.id_proceso
			and DF.tipo_periodo = base.tipo_periodo
			and DF.fecha_hasta <> base.fecha_hasta_mes_ant
			and DF.fecha_desde = (select max(fecha_desde)
					from etl.prc_df_verificacion b 
					where b.id_proceso = DF.id_proceso
						and b.tipo_periodo = DF.tipo_periodo
						and month(b.fecha_desde) = base.mes_ref)
		

		-- Borrar los 
		delete from etl.prc_df_verificacion
		where verif_activa = 'N'
			and month(fecha_desde) = month(@fechaProceso)
			and final_verificacion is null

		-- Insertar 
		insert into etl.prc_df_verificacion (
			id_proceso, tipo_periodo, fecha_desde, fecha_hasta,
			verif_activa)
		select id_proceso, tipo_periodo, fecha_desde, fecha_hasta,
			'N' as verif_activa
		from #verificacion a
		where not exists (select 1 from etl.prc_df_verificacion b
					where b.id_proceso = a.id_proceso
						and b.tipo_periodo = a.tipo_periodo
						and b.fecha_desde = a.fecha_desde)
		order by id_proceso, tipo_periodo, fecha_desde

		-- Actualizar (Por las Dudas)
		update DF
		set fecha_hasta = b.fecha_hasta
		from etl.prc_df_verificacion DF,
			#verificacion b
		where b.id_proceso = DF.id_proceso
			and b.tipo_periodo = DF.tipo_periodo 
			and b.fecha_desde = DF.fecha_desde
			and b.fecha_hasta <> DF.fecha_hasta

		commit transaction

		return 1

	end try
	begin catch
		if xact_state() <> 0
			rollback transaction

		return -1
	end catch
	-- etl.prc_gen_verificacion_df '2015-10-16'
end
GO
/****** Object:  StoredProcedure [etl].[prc_iniciar_proceso]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_iniciar_proceso]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_iniciar_proceso] (
	@idProceso numeric(15),
	@idBatch numeric(15) output)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 14-03-2014
	-- Description:	Iniciar Proceso
	-- =============================================
	begin try

		declare @parametros varchar(500),
			@proceso varchar(100),
			@msg varchar(250)

		begin transaction

		select @proceso = proceso,
			@parametros = parametro_seq
		from etl.prc_proceso
		where id_proceso = @idProceso

		insert into etl.prc_proceso_batch (
			id_proceso, estado, parametros, 
			mensaje)	
		values (@idProceso, 'IN', @parametros,
			'Proceso Inicidado.')

		set @idBatch = SCOPE_IDENTITY()

		-- Log Inicial del Proceso
		set @msg = replicate('=', 60)
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = 'Proceso: ' + @proceso + ' - (' + cast(@idProceso as varchar)+ ')'
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = 'Batch ID: ' + substring(cast (@idBatch + 100000000 as varchar), 2, 8)
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = 'Fecha Proceso: ' + convert(varchar, getdate(), 103)
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = 'Parametros: ' + isnull(@parametros,'{Ninguno}')
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = replicate('=', 60)
		exec etl.prc_log_batch @idBatch, @msg

		commit transaction

		-- OK
		return 1

	end try

	begin catch
		if @@trancount > 0
			rollback transaction

		PRINT '** ERROR AL INICIAR PROCESO: ' + ERROR_MESSAGE()

		-- ERROR
		return -1
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_log_batch] (
	@idBatch numeric(15),
	@mensaje varchar(1000))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 14-03-2014
	-- Description:	Registrar Logs de Batch
	-- =============================================


	if @idBatch <= 0
		begin
			print @mensaje
			return 
		end

	begin try
		begin tran
		if exists (select 1 
			from etl.prc_proceso_batch_log 
			where id_batch = @idBatch)
				insert into etl.prc_proceso_batch_log (
					id_batch, id_linea, mensaje )
				select @idBatch, max(id_linea) + 1, @mensaje
				from etl.prc_proceso_batch_log
				where id_batch = @idBatch
		else 
			insert into etl.prc_proceso_batch_log (
				id_batch, id_linea, mensaje )
			values (@idBatch, 1, @mensaje)
				
		commit tran
	end try

	begin catch
		if @@trancount > 0
			rollback tran
		PRINT '** ERROR: ' + ERROR_MESSAGE()
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch_mail]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_log_batch_mail]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_log_batch_mail] (
	@idBatch numeric(15),
	@mensaje varchar(1000))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 30-09-2015
	-- Description:	Registrar Logs de Mail de Usuario
	-- =============================================


	/*if @idBatch = 0
		begin
			print @mensaje
			return 
		end*/

	begin try
		begin tran
		if exists (select 1 
			from etl.prc_mail_usuario_info
			where id_batch = @idBatch)
				insert into etl.prc_mail_usuario_info (
					id_batch, id_linea, mensaje )
				select @idBatch, max(id_linea) + 1, @mensaje
				from etl.prc_mail_usuario_info
				where id_batch = @idBatch
		else 
			insert into etl.prc_mail_usuario_info (
				id_batch, id_linea, mensaje )
			values (@idBatch, 1, @mensaje)
				
		commit tran
	end try

	begin catch
		if @@trancount > 0
			rollback tran
		PRINT '** ERROR LOG MAIL: ' + ERROR_MESSAGE()
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_dts]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_dts]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_log_error_dts] (
	@idBatch numeric(15) = 0,
	@msgError varchar(500),
	@nomPaquete varchar(200))
as
begin
	declare @donde varchar(200), @tipoError varchar(30)
	begin try
		begin tran logError

		set @tipoError = 'DTSX'
		set @donde = 'Package: ' + isnull(@nomPaquete,'<null>')

		insert into etl.prc_errores (tipo_error, donde, mensaje_error, id_batch)
		values (@tipoError, @donde,  @msgError, @idBatch)

		commit tran logError

		-- Registra en Archivo
		exec etl.prc_Logging @idBatch, @msgError

	end try
	begin catch
		if XACT_STATE () = -1
			rollback tran logError
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_seq]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_seq]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_log_error_seq] (
	@msgError varchar(500))
as
begin
	declare @msgRaise varchar(500)
	declare @donde varchar(200), @tipoError varchar(30)
	begin try
		begin tran logError

		set @tipoError = 'SECUENCIA'
		set @donde = 'Ejecucion de Secuencia de Procesos'

		insert into etl.prc_errores (tipo_error, donde, mensaje_error, id_batch)
		values (@tipoError, @donde,  @msgError, null)

		commit tran logError

	end try
	begin catch
		if XACT_STATE () = -1
			rollback tran logError
	end catch

	set @msgRaise = @msgError + ' (' + @donde + ')'
	raiserror(@msgRaise, 16, 1) WITH NOWAIT

end
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_sp]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_log_error_sp]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_log_error_sp] (
	@tipo_error varchar(30) = 'GENERAL',
	@idBatch numeric(15) = 0)

as
begin
	declare @msgRaise varchar(500)
	declare @msg_error varchar(500), @donde varchar(255)
	begin try

	     IF ERROR_NUMBER() IS NULL
            RETURN

		IF XACT_STATE() = -1
		BEGIN
            PRINT 'Cannot log error since the current transaction is in an uncommittable state. ' 
                + 'Rollback the transaction before executing uspLogError in order to successfully log error information.';
            RETURN
        END;

		set @msg_error = '*** ERROR: ('+convert(varchar,ERROR_NUMBER())+') ' + ERROR_MESSAGE()
		set @donde = 'SP: ' + ERROR_PROCEDURE() + ', Linea ' + convert(varchar,ERROR_LINE()) + '.'


		-- Registra en Archivo
		exec etl.prc_Logging @idBatch, @msg_error
	
		if @tipo_error is null
			set @tipo_error = 'GENERAL'

		insert into etl.prc_errores (tipo_error, donde, mensaje_error, id_batch)
		values (@tipo_error, @donde,  @msg_error, @idBatch)
	
	
	end try
	begin catch
		PRINT '** ERROR: ' + ERROR_MESSAGE();
	end catch

	set @msgRaise = @msg_error + ' (' + @donde + ')'
	raiserror(@msgRaise, 16, 1) WITH NOWAIT
end
GO
/****** Object:  StoredProcedure [etl].[prc_Logging]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_Logging]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_Logging] (
	@idBatch numeric(15),
	@Mensaje varchar(500))
as
begin


	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31/03/2014
	-- Description:	Logs ETLs
	-- =============================================
	-- 31/03/2014 - Version Inicial
	-- 14/05/2018 - LS - Ajuste

	set nocount on

	declare @cmd varchar(1000),
		@linea varchar(1000),
		@archivo varchar(255),
		@out varchar(5)

	begin try

		-- Obtener Nombre de Archivo
		set @archivo = etl.fn_getNombreArchivoLog( @idBatch, 'MAP' )
		set @out = '>> '
	
		if @Mensaje is null or ltrim(rtrim(@Mensaje)) = ''
			set @Mensaje =  ' '
		
		-- Inicio
		if substring(@Mensaje , 1, 2 ) = '>>'
		begin
			set @Mensaje = ltrim(rtrim(replace(@Mensaje, '>>', '')))
			set @out = '> '
			--set @linea =  convert(varchar, getdate(), 20) + '|' + replicate('-', 60)
			--set @cmd = 'echo "' +@linea+ '" > ' + @archivo
			--exec xp_cmdshell @cmd, no_output 

		end

		-- Fin
		if substring(@Mensaje , 1, 2 ) = '<<'
		begin
			set @Mensaje = ltrim(rtrim(replace(@Mensaje, '<<', '')))
			--set @linea =  convert(varchar, getdate(), 20) + '|' + replicate('-', 60)
			--set @cmd = 'echo "' +@linea+ '" >> ' + @archivo
			--exec xp_cmdshell @cmd, no_output 
		end 

		-- Mensaje
		if @idBatch <= 0
			print  @Mensaje
		set @linea =  convert(varchar, getdate(), 13) + '|' + @Mensaje
		set @cmd = 'echo "' +@linea+ '" '+ @out + @archivo
		exec xp_cmdshell @cmd, no_output 
	end try
	begin catch
		print '*** ERROR: ' + ERROR_MESSAGE()
	end catch
end
-- etl.prc_Logging 0, 1, 'Este es una prueba de un logging'
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_datos]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_datos]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_reg_ctrl_datos](
	@tipoControl varchar(20),
	@descripcion varchar(255),
	@registroCtrlProceso varchar(50),
	@idBatch numeric(15) = 0)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21-05-2014
	-- Description:	Registra Control de Datos (Generico)
	-- =============================================
	-- 21/05/2014 - Version Inicial

	begin try
		declare @idDia numeric(10),
			@idControl numeric(15)

		if isnull(@idBatch,-1) < 0
			set @idBatch = 0
		
		begin tran

		-- Registrar Control
		select @idControl = max(id_control)
		from etl.prc_ctrl_datos
		where registro_ctrl_proceso = @registroCtrlProceso
			and tipo_control = @tipoControl 

		if isnull(@idControl,0) > 0
			begin
				update etl.prc_ctrl_datos
				set fecha_control = getdate(),
					id_batch = @idBatch,
					descripcion = @descripcion
				where id_Control = @idControl
			end						
		else
			begin
				insert into etl.prc_ctrl_datos (
					tipo_control, descripcion, id_batch, 
					registro_ctrl_proceso)
				values (@tipoControl, isnull(@descripcion,'<Sin Descripcion>'), @idBatch, 
					@registroCtrlProceso)

				set @idControl = SCOPE_IDENTITY()
			end

		commit tran

		return isnull(@idControl,0)
	end try
	begin catch
		rollback tran
		return -1
	end catch
end 
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_isk_FT]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_isk_FT]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_reg_ctrl_isk_FT] (
	@fechaControl datetime,
	@procesoFT varchar(60) = '',
	@corridaActual bit = 1)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 06-04-2014
	-- Description:	Registra Control de Datos (ISK) para la FTs (o una especificada)
	-- ==========================================================================================
	-- 11/04/2014 - Version Inicial
	-- 23/04/2014 - LS - Agrega Control ISK Horas Facturadas
	-- 28/04/2014 - LS - Agrega Control ISK Nomina (Con Exclusiones...)
	-- 07/05/2014 - LS - Correccion por cambios en FT Horas Facturadas
	-- 03/06/2014 - LS - Restructuracion de Proceso (Agrega ISK Pedidos AE)
	-- 30/06/2014 - LS - Agrega Control ISK Previsiones
	-- 16/07/2014 - LS - Agrega Control ISK Productividad
	-- 13/08/2014 - LS - Saco FT NOmina
	-- 05/12/2014 - LS - Columnas en Ft Visitas
	-- 16/01/2015 - LS - Columnas en Ft Cotizaciones
	-- 27/08/2015 - LS - Ajuste en Cotizacion Persona x Mail
	-- 15/09/2015 - LS - Ajuste en Cotizacion (Error de Conversion)
	-- 31/05/2019 - LS - Ajuste en Cotizacion (Quita Fecha de Activacion = 0)
	-- 15/07/2019 - LS - Quita Ref. a tablas Stages de PLanning

	declare @idDia numeric(10), @idSemana numeric(10), @idMes numeric(10),@descripcion varchar(255),
		@sql varchar(max), @idControl numeric(15), @registroCtrlProceso varchar(50), @cantidad numeric(10),
		@archivo_stg varchar(100), @archivo_rsm varchar(100), @tipoControl varchar(5), @tblStage varchar(128),
		@idProceso numeric(15), @proceso varchar(50), @idBatch numeric(15), @ExiteCtrl bit,  @tblRSM varchar(128),
		@idPeriodo numeric(10), @periodicidad char(1)

	create table #seeds (
		id_control numeric(15),
		parametro varchar(30) COLLATE database_default,
		valor varchar(500) COLLATE database_default,
		cantidad numeric(10))

	set nocount on
	begin try

		-- Obtener Valores Ref.
		select @idDia = id_dia, 
			@idSemana = id_semana, 
			@idMes = id_mes
		from dbo.DTPO_LK_DIAS
		where fecha = etl.fn_truncFecha( @fechaControl )

		set @tipoControl = 'ISK'

		-- Procesos Que Corrieron en la Fecha de Control....
		declare lc_prc_cursor cursor for
		select a.id_proceso, a.proceso, max(isnull(b.id_batch,0)) id_batch,
			max(periodicidad)
		from  etl.prc_proceso a
			left outer join etl.prc_proceso_batch b on a.id_proceso = b.id_proceso
		where 1=1
			and (etl.fn_truncFecha( @fechaControl ) = etl.fn_truncFecha(b.fecha_inicio) or @corridaActual = 0)
			and (a.proceso = @procesoFT or isnull(@procesoFT,'') = '')
			and a.tipo_proceso = 'FT'
		group by  a.id_proceso, a.proceso
		order by 3

		open lc_prc_cursor
		fetch next from lc_prc_cursor into @idProceso, @Proceso, @idBatch, @periodicidad
		while @@fetch_status = 0
			begin

				-- Determinar Periodo de Proceso
				set @idPeriodo = case 
					when @proceso = 'FT_NOMINA' then @idMes
					when @periodicidad = 'D' then @idDia
					when @periodicidad = 'S' then @idSemana
					when @periodicidad = 'M' then @idMes
						else @idMes
					end

				-- Datos (Control ISK para todos es Mensual)
				set @registroCtrlProceso = @proceso + '-' + cast(@idPeriodo as varchar)
				set @archivo_stg = lower(@proceso) + '_stg_' + cast(@idPeriodo as varchar) + '.log'
				set @archivo_rsm = lower(@proceso) + '_rsm_' + cast(@idPeriodo as varchar) + '.log'
				set @descripcion = 'Inconsistencias SKs en ' + @proceso
				set @tblRSM = '##isk_rsm_' + lower(@proceso)

				-- Existe Control ISK
				select @ExiteCtrl = case when isnull(id,0) > 0 then 1 else 0 end,
					@idControl = id
				from ( select max(id_control) as id
					from etl.prc_ctrl_datos
					where registro_ctrl_proceso = @registroCtrlProceso
						and tipo_control = @tipoControl ) x

				truncate table #seeds

				if exists (select 1 from tempdb..sysobjects where name like @tblRSM)
					execute ('drop table ' + @tblRSM)

				-----------------------------------------------
				-- 'FT_VISITA'
				-----------------------------------------------
				if @proceso = 'FT_VISITA'
					begin 
						
						set @tblStage = 'ST_FT_VISITAS'

						-- Detectar ISK
						set @sql = '
							select a.id_stage,
								a.cod_visita, a.id_mes, 
								a.id_dia, b.fecha_visita,
								a.id_cliente, b.cuit_cliente,
								a.id_sucursal, b.cod_sucursal,
								a.id_tipo_visita, b.cod_tipo_visita,
								a.id_creador, b.cod_cc +''-''+b.cod_persona as cod_creador,
								a.id_lista, b.cod_cc,
								a.id_cc, a.id_usuario, b.cod_persona
							into '+@tblRSM+'
							from stg.PD_FT_VISITAS a,
								stg.ST_FT_VISITAS b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_cliente = -1
									or id_sucursal = -1
									or id_tipo_visita = -1
									or id_creador = -1
									or id_lista = -1
									or id_usuario = -1
									or id_cc = -1)'
						execute (@sql)

						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''CUIT_CLIENTE'' as campo_ref, cuit_cliente as valor
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_SUCURSAL'', cod_sucursal
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_TIPO_VISITA'', cod_tipo_visita
										from '+@tblRSM+'
										where id_tipo_visita = -1
										union all
										select id_stage, ''COD_CREADOR'', cod_creador
										from '+@tblRSM+'
										where id_creador = -1										
										union all
										select id_stage, ''COD_CC_LISTA'', cod_cc
										from '+@tblRSM+'
										where id_lista = -1 
										union all
										select id_stage, ''COD_CC'', cod_cc
										from '+@tblRSM+'
										where id_cc = -1 
										union all
										select id_stage, ''COD_USUARIO'', cod_persona
										from '+@tblRSM+'
										where id_usuario = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end 
					end

				-----------------------------------------------
				-- 'FT_PLANNING'
				-----------------------------------------------
				if @proceso = 'FT_PLANNING'
					begin 
						
						set @tblStage = 'ST_FT_PLANNING_LEGAJO'

						-- Detectar ISK
						set @sql = '
							select  a.id_stage,
								a.id_semana,
								cast(b.periodo as varchar) + ''-''+ cast(b.semana as varchar) as semana,
								a.id_sucursal,
								b.sucursal,
								a.id_cliente,
								b.cliente,
								a.id_empresa,
								b.empresa, 
								a.id_perfil,
								b.tipo_categ,
								a.id_tipo_serv_detalle
							into '+@tblRSM+' 
							from stg.PD_FT_PLANNING_LEGAJO a,
								stg.ST_FT_PLANNING_LEGAJO b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (a.id_semana <= 0
									or a.id_cliente = -1
									or a.id_empresa = -1
									or a.id_perfil = -1
									or a.id_tipo_serv_detalle <= 0)'
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''SEMANA'' as campo_ref, semana as valor
										from '+@tblRSM+'
										where id_semana = -1
										union all
										select id_stage, ''COD_CLIENTE'' as campo_ref, cast(cliente as varchar)
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_EMPRESA'', cast(empresa as varchar)
										from '+@tblRSM+'
										where id_empresa = -1
										union all
										select id_stage, ''COD_PERFIL'', tipo_categ
										from '+@tblRSM+'
										where id_perfil = -1
										union all
										select id_stage, ''COD_TIPO_SERV_DETALLE'', ''<Calculado>''
										from '+@tblRSM+'
										where id_tipo_serv_detalle <= 0) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_MARKET_SHARE'
				-----------------------------------------------
				if @proceso = 'FT_MARKET_SHARE'
					begin 
						
						set @tblStage = 'ST_FT_MARKET_SHARE'

						-- Detectar ISK
						set @sql = '
							select a.id_stage,
								a.id_mes,
								a.id_empresa,
								a.id_sucursal,
								b.codigo_sucursal as cod_sucursal
							into '+@tblRSM+'
							from stg.PD_FT_MARKET_SHARE a,
								stg.ST_FT_MARKET_SHARE b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_sucursal = -1 ) '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_EMPRESA'', ''<Calculado>''
										from '+@tblRSM+'
										where id_empresa = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_ESTADO_GESTION'
				-----------------------------------------------
				if @proceso = 'FT_ESTADO_GESTION'
					begin 
						
						set @tblStage = 'ST_FT_ESTADO_GESTION'

						-- Detectar ISK
						set @sql = '
							select  a.id_stage, a.id_mes, a.id_sucursal, b.cod_sucursal, 
								a.id_linea, b.cod_linea_gestion, 
								b.tipo
							into '+@tblRSM+' 
							from stg.PD_FT_ESTADO_GESTION a,
								stg.ST_FT_ESTADO_GESTION b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_sucursal = -1
									or id_linea = -1) '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_LINEA_GESTION'', cod_linea_gestion
										from '+@tblRSM+'
										where id_linea = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_COTIZACIONES'
				-----------------------------------------------
				if @proceso = 'FT_COTIZACIONES'
					begin 
						
						set @tblStage = 'ST_FT_COTIZACIONES'

						-- Detectar ISK
						set @sql = '
							select  a.id_stage, a.nro_cotizacion,
								a.id_sucursal, b.codigo_sucursal as cod_sucursal,
								a.id_subcliente, b.codigo_subcliente as cod_subcliente, b.codigo_empresa as cod_empresa,
								a.id_cliente, b.codigo_cliente as cod_cliente, 
								a.id_tipo_serv_detalle, b.codigo_tipo_serv as cod_tipo_serv_detalle,
								a.id_tipo_cotiz, b.tipo_cotizacion as cod_tipo_cotiz,
								a.id_term_pago, b.codigo_termino_pago as cod_term_pago,
								a.id_status_cotiz, b.status_cot_codigo as cod_status_cotiz,
								a.id_contrato_ai, b.contrato_ai_codigo as cod_contrato_ai,
								a.id_fecha_activacion, b.fecha_activacion,
								a.id_fecha_creacion, b.fecha_creacion,
								b.codigo_sucursal as cod_cc,
								a.id_cc, a.id_usuario, b.mail_creador
							into '+@tblRSM+' 
							from stg.PD_FT_COTIZACIONES a,
								stg.ST_FT_COTIZACIONES b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
							and (id_sucursal = -1
								or id_subcliente = -1
								or id_cliente = -1
								or id_tipo_serv_detalle = -1
								or id_tipo_cotiz = -1
								or id_term_pago = -1
								or id_status_cotiz = -1
								or id_contrato_ai = -1
								or id_fecha_creacion = 0
								or id_cc = -1
								or id_usuario = -1)
							order by 1 '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cast(cod_sucursal as varchar)  valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1
										union all
										select id_stage, ''COD_CLIENTE'', cast(cod_cliente as varchar)
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_TIPO_SERV_DETALLE'', cod_tipo_serv_detalle
										from '+@tblRSM+'
										where id_tipo_serv_detalle = -1
										union all
										select id_stage, ''COD_TIPO_COTIZ'', cod_tipo_cotiz
										from '+@tblRSM+'
										where id_tipo_cotiz = -1
										union all
										select id_stage, ''COD_TERM_PAGO'', cod_term_pago
										from '+@tblRSM+'
										where id_term_pago = -1
										union all
										select id_stage, ''COD_STATUS_COTIZ'', cod_status_cotiz
										from '+@tblRSM+'
										where id_status_cotiz = -1
										union all
										select id_stage, ''COD_CONTRATO_AI'', cod_contrato_ai
										from '+@tblRSM+'
										where id_contrato_ai = -1
										union all
										select id_stage, ''FECHA_ACTIVACION'', convert(varchar, isnull(fecha_activacion,''01/01/1900''), 103)
										from '+@tblRSM+'
										where id_fecha_activacion <= 0
										union all
										select id_stage, ''FECHA_CREACION'', convert(varchar, isnull(fecha_creacion,''01/01/1900''), 103)
										from '+@tblRSM+'
										where id_fecha_creacion <= 0
										union all
										select id_stage, ''COD_CC'', cast( cod_sucursal  as varchar)
										from '+@tblRSM+'
										where id_cc = -1 
										union all
										select id_stage, ''MAIL_CREADOR'', mail_creador
										from '+@tblRSM+'
										where id_usuario = -1 																				
										) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_FACTURACION_TS'
				-----------------------------------------------
				if @proceso = 'FT_FACTURACION_TS'
					begin 
						
						set @tblStage = 'ST_FT_FACTURACION_TS'

						-- Detectar ISK
						set @sql = '
							select a.id_stage, 
								a.id_mes,
								a.id_empresa, 
								a.id_subcliente, b.codigo_subcliente as cod_subcliente,
								a.id_sucursal, isnull(b.codigo_sucursal,''<null>'') as cod_sucursal,
								a.id_condic_subclte, b.condicion_cliente,
								a.id_tipo_serv_detalle, b.tipo_serv as cod_tipo_serv_detalle,
								a.id_cliente, b.customer_id as cod_customer
							into '+@tblRSM+' 
							from stg.PD_FT_FACTURACION_TS a,
								stg.ST_FT_FACTURACION_TS b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and not (importe_fact = 0
									and mb_porc = 0
									and mb_monto = 0)
								and (a.id_sucursal <= 0
									or id_condic_subclte = -1
									or id_subcliente = -1
									or id_tipo_serv_detalle = -1
									or id_cliente = -1)
								and id_condic_subclte > 0 '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal <= 0
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1
										union all
										select id_stage, ''COD_CUSTOMER'', cod_customer
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_TIPO_SERV_DETALLE'', cod_tipo_serv_detalle
										from '+@tblRSM+'
										where id_tipo_serv_detalle = -1
										union all
										select id_stage, ''COD_CONDICION_CLIENTE'', condicion_cliente
										from '+@tblRSM+'
										where id_condic_subclte = -1										
										) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_PMC'
				-----------------------------------------------
				if @proceso = 'FT_PMC'
					begin 
						
						set @tblStage = 'ST_FT_PMC'

						-- Detectar ISK
						set @sql = '
							select a.id_stage, 
								a.id_mes, a.id_dia,
								a.id_sucursal, b.cod_sucursal,
								a.id_subcliente, b.cod_subcliente,
								a.id_empresa, b.cod_empresa,
								a.id_cliente, b.cod_cliente
							into '+@tblRSM+' 
							from stg.PD_FT_PMC a,
								stg.ST_FT_PMC b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_sucursal = -1
									or id_subcliente = -1
									or id_cliente = -1
									or id_empresa = -1) '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1
										union all
										select id_stage, ''COD_CLIENTE'', cod_cliente
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_EMPRESA'', cod_empresa
										from '+@tblRSM+'
										where id_empresa = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_HORAS_FACTURADAS'
				-----------------------------------------------
				if @proceso = 'FT_HORAS_FACTURADAS'
					begin 
						
						set @tblStage = 'ST_FT_HORAS_FACTURADAS'

						-- Detectar ISK
						set @sql = '
							select a.id_stage, 
								a.id_mes, b.periodo,
								a.id_sucursal, b.cod_sucursal,
								a.id_subcliente, b.cod_subcliente,
								a.id_empresa, b.cod_empresa,
								a.id_cliente, b.cod_cliente,
								a.id_concepto_hf, b.cod_concepto_hf,
								a.id_tipo_hora, b.cod_tipo_hora_distr
							into '+@tblRSM+' 
							from stg.PD_FT_HORAS_FACTURADAS a,
								stg.ST_FT_HORAS_FACTURADAS b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_sucursal = -1
									or id_subcliente = -1
									or id_cliente = -1
									or id_empresa = -1
									or id_concepto_hf = -1
									or id_tipo_hora = -1) '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1
										union all
										select id_stage, ''COD_CLIENTE'', cod_cliente
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_EMPRESA'', cod_empresa
										from '+@tblRSM+'
										where id_empresa = -1
										union all
										select id_stage, ''COD_CONCEPTO_HF'', cod_concepto_hf
										from '+@tblRSM+'
										where id_concepto_hf = -1
										union all
										select id_stage, ''COD_TIPO_HORA'', cod_tipo_hora_distr
										from '+@tblRSM+'
										where id_tipo_hora = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				
				-----------------------------------------------
				-- 'FT_PEDIDOS'
				-----------------------------------------------
				if @proceso = 'FT_PEDIDOS'
					begin 
						
						set @tblStage = 'ST_FT_PEDIDOS'

						-- Detectar ISK
						set @sql = '
							select a.id_stage, a.cod_pedido, 
								a.id_cliente, b.nro_cuit,
								a.id_sucursal, b.cod_sucursal,
								a.id_perfil, b.cod_perfil, 
								a.id_tipo_serv_detalle,
								a.id_producto_ped, b.cod_producto_ped,
								a.id_cargo_ped, b.cod_cargo_ped,
								a.id_usuario_ped, b.cod_usuario_ped,
								a.id_estado_ped, b.cod_estado_ped
							into '+@tblRSM+' 
							from stg.PD_FT_PEDIDOS a,
								stg.ST_FT_PEDIDOS b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_cliente = -1
									or id_sucursal = -1
									or id_perfil = -1
									or id_tipo_serv_detalle = -1
									or id_producto_ped = -1
									or id_cargo_ped = -1
									or id_usuario_ped = -1
									or id_estado_ped = -1) '
						execute (@sql)
								
						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_SUCURSAL'' as campo_ref, cod_sucursal as valor
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''NRO_CUIT'', nro_cuit
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_PERFIL'', cod_perfil
										from '+@tblRSM+'
										where id_perfil = -1
										union all
										select id_stage, ''COD_TIPO_SERV_DETALLE'', cod_producto_ped
										from '+@tblRSM+'
										where id_tipo_serv_detalle = -1
										union all
										select id_stage, ''COD_PRODUCTO_PED'' , cod_producto_ped
										from '+@tblRSM+'
										where id_producto_ped = -1
										union all
										select id_stage, ''COD_CARGO_PED'', cod_cargo_ped
										from '+@tblRSM+'
										where id_cargo_ped = -1
										union all
										select id_stage, ''COD_USUARIO_PED'' , cod_usuario_ped
										from '+@tblRSM+'
										where id_usuario_ped = -1
										union all
										select id_stage, ''COD_ESTADO_PED'', cod_estado_ped
										from '+@tblRSM+'
										where id_estado_ped = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end
					end

				-----------------------------------------------
				-- 'FT_PREVISIONES'
				-----------------------------------------------
				if @proceso = 'FT_PREVISIONES'
					begin 
						
						set @tblStage = 'ST_FT_PREVISIONES'

						-- Detectar ISK
						set @sql = '
							select a.id_stage,
									a.id_mes, b.mes,
									a.id_cliente, b.cod_cliente,
									a.id_subcliente, b.cod_subcliente,
									a.id_sucursal, b.cod_sucursal,
									a.id_concepto_prev, b.cod_concepto_prev,
									b.cod_empresa as cod_empresa_nx
							into '+@tblRSM+'
							from stg.PD_FT_PREVISIONES a,
								stg.ST_FT_PREVISIONES b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_cliente = -1
									or id_sucursal = -1
									or id_concepto_prev = -1
									or id_subcliente = -1)'
						execute (@sql)

						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_CLIENTE'' as campo_ref, cod_cliente as valor
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_SUCURSAL'', cod_sucursal
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_CONCEPTO_PREV'', cod_concepto_prev
										from '+@tblRSM+'
										where id_concepto_prev = -1
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end 
					end

				-----------------------------------------------
				-- 'FT_PRODUCTIVIDAD'
				-----------------------------------------------
				if @proceso = 'FT_PRODUCTIVIDAD'
					begin 
						
						set @tblStage = 'ST_FT_PRODUCTIVIDAD'

						-- Detectar ISK
						set @sql = '
							select a.id_stage,
									a.id_mes, b.mes,
									a.id_cliente, b.cod_cliente,
									a.id_subcliente, b.cod_subcliente,
									a.id_sucursal, b.cod_sucursal,
									a.id_concepto_prd, b.cod_concepto_prd,
									b.cod_empresa as cod_empresa_nx
							into '+@tblRSM+'
							from stg.PD_FT_PRODUCTIVIDAD a,
								stg.ST_FT_PRODUCTIVIDAD b
							where a.id_stage = b.id_stage
								and (b.fecha_carga = convert(varchar, getdate(), 103) or '+cast(@corridaActual as varchar)+' = 0)
								and (id_cliente = -1
									or id_sucursal = -1
									or id_concepto_prd = -1
									or id_subcliente = -1)'
						execute (@sql)

						set @cantidad = @@rowcount
						if @cantidad > 0
							begin
								-- Registrar/Actualizar Control
								exec @idControl = etl.prc_reg_ctrl_datos @tipoControl, @descripcion, @registroCtrlProceso, @idBatch

								-- Registrar Valores (Seed)
								set @sql = '
									insert into #seeds (id_control, parametro, valor, cantidad)
									select '+cast(@idControl as varchar) +' as id_control, campo_ref, valor, count(1) as cantidad
									from (
										select id_stage, ''COD_CLIENTE'' as campo_ref, cod_cliente as valor
										from '+@tblRSM+'
										where id_cliente = -1
										union all
										select id_stage, ''COD_SUCURSAL'', cod_sucursal
										from '+@tblRSM+'
										where id_sucursal = -1
										union all
										select id_stage, ''COD_CONCEPTO_PRD'', cod_concepto_prd
										from '+@tblRSM+'
										where id_concepto_prd = -1
										union all
										select id_stage, ''COD_SUBCLIENTE'', cod_subcliente
										from '+@tblRSM+'
										where id_subcliente = -1 ) a
									group by campo_ref, valor'
								execute (@sql)
							end 
					end

				---------------------------------
				-- Para cada Proceso ISK
				---------------------------------
				if @idControl > 0
					update ISK
					set fecha_registro = getdate(),
						verif = 'S'
					from etl.prc_ctrl_sks ISK
					where id_control = @idControl
						and not exists (select 1 from #seeds b 
							where b.id_control = ISK.id_control 
								and b.parametro = ISK.parametro
								and b.valor = ISK.valor)
					
				if @ExiteCtrl = 0
					update etl.prc_ctrl_datos
					set archivo_stg = @archivo_stg, 
						archivo_rsm = @archivo_rsm
					where id_control = @idControl

				--print @Proceso
				--select * from #seeds 
				--return
								
				if @cantidad > 0
					begin
						insert into etl.prc_ctrl_sks (
							id_control, parametro, valor, cantidad)
						select id_control, parametro, isnull(valor,'<null>'), cantidad 
						from #seeds a
						where not exists (select 1 from etl.prc_ctrl_sks b 
										where b.id_control = a.id_control 
											and b.parametro = a.parametro
											and b.valor = a.valor)

						update ISK
						set cantidad = b.cantidad,
							verif = 'N',
							fecha_registro = getdate()
						from etl.prc_ctrl_sks ISK, #seeds b
						where b.id_control = ISK.id_control 
							and b.parametro = ISK.parametro
							and b.valor = ISK.valor
					end

				-- Exportar Stage (STG)
				set @sql = 'select * from '+ db_name() +'.stg.'+ @tblStage +' b '
				set @sql = @sql + 'where exists (select 1 from '+@tblRSM+' a where a.id_stage = b.id_stage)'
				exec etl.prc_exportar_isk @proceso, @tblStage , @sql, @archivo_stg

				-- Exportar Resumen (RSM)
				set @sql = 'select * from '+@tblRSM+' order by id_stage'
				exec etl.prc_exportar_isk @proceso, @tblRSM, @sql, @archivo_rsm
		
				-- Next Cursor
				fetch next from lc_prc_cursor into @idProceso, @Proceso, @idBatch, @periodicidad
			end
		close lc_prc_cursor
		deallocate lc_prc_cursor
	end try

	begin catch
		if xact_state() <> 0
			rollback

		if cursor_status('global','lc_prc_cursor') = 1
			begin
				close lc_prc_cursor
				deallocate lc_prc_cursor
			end 

		exec etl.prc_log_error_sp 'CTROL-ISK', 0	
	end catch
	-- set dateformat ymd
	-- etl.prc_reg_ctrl_isk_FT '2014-06-30', 'FT_PRODUCTIVIDAD', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-06-30', 'FT_PREVISIONES', 1
	-- etl.prc_reg_ctrl_isk_FT '2014-12-05', 'FT_VISITA', 1
	-- etl.prc_reg_ctrl_isk_FT '2014-06-03', 'FT_NOMINA', 1
	-- etl.prc_reg_ctrl_isk_FT '2019-05-31', 'FT_COTIZACIONES', 0
	-- etl.prc_reg_ctrl_isk_FT '2019-07-15', 'FT_PLANNING', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-05-30', 'FT_MARKET_SHARE', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-05-30', 'FT_ESTADO_GESTION', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-05-30', 'FT_FACTURACION_TS', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-05-30', 'FT_PMC', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-11-18', 'FT_HORAS_FACTURADAS', 0
	-- etl.prc_reg_ctrl_isk_FT '2014-05-30', 'FT_PEDIDOS', 0
	-- etl.prc_reg_ctrl_isk_FT '2018-10-21', default, 0
end
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_mapping]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_mapping]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_reg_ctrl_mapping](
	@descripcion varchar(255),
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 08-04-2014
	-- Description:	Registra Control de Datos Desde Mapping
	-- =============================================
	-- 11/04/2014 - Version Inicial

	begin try
		declare @Proceso varchar(60), @idDia numeric(10),
			@registroCtrlProceso varchar(30), 
			@idControl numeric(15)

		if isnull(@idBatch,0) <= 0
			return

		select @Proceso = a.proceso
		from etl.prc_proceso a,
			etl.prc_proceso_batch b
		where a.id_proceso = b.id_proceso
			and b.id_batch = @idBatch

		set @idDia = etl.fn_LookUpIdDia( getdate() )

		-- Datos
		set @registroCtrlProceso = @Proceso + '-' + cast(@idDia  as varchar)
		
		begin tran

		-- Registrar Control
		select @idControl = max(id_control)
		from etl.prc_ctrl_datos
		where registro_ctrl_proceso = @registroCtrlProceso
			and tipo_control <> 'ISK'

		if isnull(@idControl,0) > 0
			begin
				update etl.prc_ctrl_datos
				set fecha_control = getdate(),
					id_batch = @idBatch
				where id_Control = @idControl
			end						
		else
			begin
				insert into etl.prc_ctrl_datos (
					tipo_control, descripcion, id_batch, 
					registro_ctrl_proceso)
				values ('MAP', isnull(@descripcion,'<Sin Descripcion>'), @idBatch, 
					@registroCtrlProceso)

				set @idControl = SCOPE_IDENTITY()
			end

		commit tran

		return isnull(@idControl,0)
	end try
	begin catch
		rollback tran
		return -1
	end catch
end 
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_varios]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_reg_ctrl_varios]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_reg_ctrl_varios] (
	@fechaControl datetime,
	@runControl varchar(30) = '')
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 21-05-2014
	-- Description:	Controles Varios
	-- ==========================================================================================
	-- 21/05/2014 - Version Inicial
	-- 24/07/2014 - LS - Se Agrega Control de Concepto Improductivos
	
	set nocount on

	begin try

		declare @runAll bit,
			@registroCtrlProceso varchar(50),
			@tipoControl varchar(20),
			@descripcion varchar(255),
			@sql varchar(1000),
			@periodo varchar(6),
			@ArchivoLog varchar(100)

		if etl.fn_trim(isnull(@runControl,'')) = ''
			set @runAll = 1
		else
			set @runAll = 0

		select @periodo = convert(varchar, id_mes)
		from dbo.DTPO_LK_DIAS
		where fecha = etl.fn_truncFecha( @fechaControl )

		-----------------------------------------------
		-- 'CTRL: CAMBIO_DF'
		-----------------------------------------------
		if @runAll = 1 or  @runControl = 'CAMBIO_DF'
			begin

				set @tipoControl = 'CDF'

				-- Cambios/Variacion en Datos Fuentes
				declare lc_cursor cursor for
				select x.proceso + '-' + cast(x.id_periodo as varchar) + '-' + 'CDF' as registro,
					'Variacion Diaria en DF de proceso ' + x.proceso + ' en periodo ' + cast(x.id_periodo as varchar) +
					' - [Anterior: '+cast(ant.cant_registros as varchar)+'][Actual: '+cast(act.cant_registros as varchar)+']'  as descripcion
				from dbo.ODS_DF_REGISTRO act,
					dbo.ODS_DF_REGISTRO ant,
					(select act.id_proceso, act.proceso, act.id_periodo, 
						act.id_registro as id_reg_actual,
						max(ant.id_registro) as id_reg_anterior
					from dbo.ODS_DF_REGISTRO ant,
						(select a.id_proceso, a.proceso, a.id_periodo, id_registro
						from dbo.ODS_DF_REGISTRO a
						where id_registro = (select max(id_registro)
										from dbo.ODS_DF_REGISTRO b
										where 1=1
											and b.fecha_registro <= @fechaControl
											and b.id_proceso = a.id_proceso)) act
					where ant.id_proceso = act.id_proceso
						and ant.id_periodo = act.id_periodo
						and ant.id_registro < act.id_registro
					group by act.id_proceso, act.proceso, act.id_periodo, act.id_registro) x
				where act.id_registro = x.id_reg_actual
					and ant.id_registro = x.id_reg_anterior
					and (ant.cant_registros <> act.cant_registros 
							and (act.cant_registros <> 0 
								or ant.cant_registros <> 0 and act.cant_registros = 0))				

				open lc_cursor
				fetch next from lc_cursor into @registroCtrlProceso, @descripcion
				while @@fetch_status = 0
					begin
				
						exec etl.prc_reg_ctrl_datos @tipoControl,
													@descripcion,
													@registroCtrlProceso


						-- next
						fetch next from lc_cursor into @registroCtrlProceso, @descripcion
					end
				close lc_cursor
				deallocate lc_cursor
			end 


		-----------------------------------------------
		-- 'CTRL: CPTO_IMPROD'
		-----------------------------------------------
		if @runAll = 1 or  @runControl = 'CPTO_IMPROD'
			begin

				set @tipoControl = 'CVR'

				if exists (select 1 from tempdb..sysobjects where name like '##cptoImprod%')
					execute ('drop table ##cptoImprod')

				select b.cod_concepto, c.cod_concepto_prd, c.desc_concepto, 
					a.id_mes, a.id_sucursal, c.cod_sucursal,
					a.id_subcliente, c.cod_subcliente, c.cod_empresa as cod_empresa_nx,
					a.id_cliente, c.cod_cliente,
					a.debe,
					a.haber,
					c.desc_categoria as categoria_asiento,
					c.origen as origen_asiento,
					c.nom_asiento,
					a.id_stage
				into ##cptoImprod
				from stg.PD_FT_PRODUCTIVIDAD a,
					dbo.DPRD_LK_CONCEPTO b,
					stg.ST_FT_PRODUCTIVIDAD c
				where b.id_concepto_prd = a.id_concepto_prd
					and a.id_stage = c.id_stage
					and b.cod_concepto = 'OTR'
					and b.id_grupo_prd = 2
					and c.cod_concepto_prd = '511003'
					and not exists (select 1 from dbo.ODS_TX_CONCEPTO_IMPRODUCTIVO x
									where x.desc_concepto_nx = c.desc_concepto)
					and exists (select 1
						from  etl.prc_proceso a,
							etl.prc_proceso_batch b
						where a.id_proceso = b.id_proceso
							and etl.fn_truncFecha(@fechaControl) = etl.fn_truncFecha(b.fecha_inicio)
							and a.proceso = 'FT_PRODUCTIVIDAD'
							and b.estado = 'OK')

				if @@rowcount > 0 
					begin
						-- Exportar
						set @sql = 'select * from ##cptoImprod order by id_stage'
						set @ArchivoLog = 'ConceptoImproductivo-'+@periodo+'.xls'
						exec etl.prc_exportar_isk 'CPTO_IMPROD', '##cptoImprod', @sql, @ArchivoLog

						-- Reg. Control
						set @registroCtrlProceso = 'ConceptoImproductivo-'+@periodo
						set @descripcion = 'Se Registraron Conceptos Improductivos Sin Clasificar.'

						exec etl.prc_reg_ctrl_datos @tipoControl,
							@descripcion,
							@registroCtrlProceso

					end 
			end 

	end try

	begin catch
		if xact_state() <> 0
			rollback

		exec etl.prc_log_error_sp 'CTROL-VRS', 0	
	end catch

	-- exec etl.prc_reg_ctrl_varios '2014-05-20'
	-- exec etl.prc_reg_ctrl_varios '2014-07-24', 'CPTO_IMPROD'

end
GO
/****** Object:  StoredProcedure [etl].[prc_reg_periodo_actual]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_reg_periodo_actual]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_reg_periodo_actual]
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 21-05-2014
	-- Description:	Registra Periodo Actual de Proceso
	-- ==========================================================================================
	-- 02/07/2014 - Version Inicial
	-- 16/07/2014 - LS - Correccion de Errores
	
	set nocount on

	begin try


		declare @idProceso numeric(15), @idPeriodo numeric(15), 
			@tipoPeriodo char(1), @tablaModelo varchar(128)

		declare lc_cursor cursor for
		select a.id_proceso, a.id_periodo, a.tipo_periodo, b.proceso
		from etl.prc_periodo_actual_proceso a,
			etl.prc_proceso b
		where a.id_proceso = b.id_proceso
		order by 1

		open lc_cursor
		fetch next from lc_cursor into @idProceso, @idPeriodo, @tipoPeriodo, @tablaModelo
		while @@fetch_status = 0
			begin

					if not exists (select 1 from dbo.ODS_TM_PERIODO_ACTUAL where tabla_modelo = @tablaModelo)
						begin
							insert into dbo.ODS_TM_PERIODO_ACTUAL (tabla_modelo, id_periodo, tipo_periodo)
							values (@tablaModelo, @idPeriodo, @tipoPeriodo)
						end
					else
						begin
							update dbo.ODS_TM_PERIODO_ACTUAL
							set id_periodo = @idPeriodo
							where tabla_modelo = @tablaModelo
								and id_periodo < @idPeriodo
						end
			
				-- next
				fetch next from lc_cursor into @idProceso, @idPeriodo, @tipoPeriodo, @tablaModelo
			end
		close lc_cursor
		deallocate lc_cursor
	end try

	begin catch
		if xact_state() <> 0
			rollback

		exec etl.prc_log_error_sp 'CTROL-VRS', 0	
	end catch

	-- exec etl.prc_reg_periodo_actual 

end

GO
/****** Object:  StoredProcedure [etl].[prc_run_control_datos]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_run_control_datos]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_run_control_datos] (
	@fechaProceso datetime)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 09-04-2014
	-- Description:	Control de Datos - Se debe correr en forma independiente cuando hayan finalizados
	--				todos los procesos del dia. Se pueden Agregar mas Controles Globales, 
	--				Indicadores de estados, etc
	--				Al Finalizar Debe Enviar un Mail de Aviso
	-- ==========================================================================================
	-- 09/04/2014 - Version Inicial
	-- 21/05/2014 - LS - Agrega SP Controles Varios
	-- 02/07/2014 - LS - Agrega SP Registrar Periodo de Procesos

	set nocount on

	-- Inconsistencias SK (ISK) de FTs
	Exec etl.prc_reg_ctrl_isk_FT @fechaProceso, default, default

	-- Registra periodos actual de algunas tablas del modelo
	Exec etl.prc_reg_periodo_actual 

	-- Correr al Final
	---------------------------------------
	-- Controles Varios
	Exec etl.prc_reg_ctrl_varios @fechaProceso, default


	-- Enviar Mail
	Exec etl.prc_enviar_mail_control @fechaProceso

end
GO
/****** Object:  StoredProcedure [etl].[prc_run_control_isk]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_run_control_isk]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_run_control_isk] (
	@fechaControl datetime,
	@actual bit = 1)
as
begin

	declare @idDia numeric(10), @idSemana numeric(10), @idMes numeric(10),
		@sql varchar(250), @idControl numeric(15), @registroCtrlProceso varchar(50),
		@archivo_stg varchar(100), @archivo_rsm varchar(100),
		@idProceso numeric(15), @proceso varchar(50), @idBatch numeric(15)

	set nocount on

	begin try

		-- Obtener Valores Ref.
		select @idDia = id_dia, 
			@idSemana = id_semana, 
			@idMes = id_mes
		from dbo.DTPO_LK_DIAS
		where fecha = etl.fn_truncFecha( @fechaControl )

		-- Procesos Que Corrieron en la Fecha de Control....
		declare lc_cursor cursor for
		select a.id_proceso, a.proceso, max(isnull(b.id_batch,0)) id_batch
		from  etl.prc_proceso a
			left outer join etl.prc_proceso_batch b on a.id_proceso = b.id_proceso
		where 1=1
			and (etl.fn_truncFecha( @fechaControl ) = etl.fn_truncFecha(b.fecha_inicio) or @actual = 0)
			and a.tipo_proceso = 'FT'
		group by  a.id_proceso, a.proceso
		order by 3


		open lc_cursor
		fetch next from lc_cursor into @idProceso, @Proceso, @idBatch
		while @@fetch_status = 0
			begin
				-----------------------------------------------
				-- 'FT_VISITA'
				-----------------------------------------------
				if @proceso = 'FT_VISITA'
				begin 

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_visita')
						drop table ##isk_rsm_visita

					select a.id_stage,
						a.cod_visita, a.id_mes, 
						a.id_dia, b.fecha_visita,
						a.id_cliente, b.cuit_cliente,
						a.id_sucursal, b.cod_sucursal,
						a.id_tipo_visita, b.cod_tipo_visita
					into ##isk_rsm_visita
					from stg.PD_FT_VISITAS a,
						stg.ST_FT_VISITAS b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (id_cliente = -1
							or id_sucursal = -1
							or id_tipo_visita = -1)

					if @@rowcount > 0 
						begin
							-- DAtos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_visita_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_visita_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'CUIT_CLIENTE' as campo_ref, cuit_cliente as valor
								from ##isk_rsm_visita
								where id_cliente = -1
								union all
								select id_stage, 'COD_SUCURSAL', cod_sucursal
								from ##isk_rsm_visita
								where id_sucursal = -1
								union all
								select id_stage, 'COD_TIPO_VISITA', cod_tipo_visita
								from ##isk_rsm_visita
								where id_tipo_visita = -1) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_VISITAS b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_visita a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_VISITAS', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_visita order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_visita', @sql, @archivo_rsm
						end
				end
		
				-----------------------------------------------
				-- 'FT_PLANNING'
				-----------------------------------------------
				if @proceso = 'FT_PLANNING'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_planning')
						drop table ##isk_rsm_planning

					select  a.id_stage,
						a.id_semana,
						cast(b.periodo as varchar) + '-'+ cast(b.semana as varchar) as semana,
						a.id_sucursal,
						b.sucursal,
						a.id_cliente,
						b.cliente,
						a.id_empresa,
						b.empresa, 
						a.id_perfil,
						b.tipo_categ,
						a.id_tipo_serv_detalle
					into ##isk_rsm_planning
					from stg.PD_FT_PLANNING a,
						stg.ST_FT_PLANNING b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (a.id_semana <= 0
							or a.id_cliente = -1
							or a.id_empresa = -1
							or a.id_perfil = -1
							or a.id_tipo_serv_detalle <= 0)				

					if @@rowcount > 0 
						begin
							-- Datos
							set @registroCtrlProceso = @proceso + '-' + cast(@idSemana as varchar)
							set @archivo_stg = 'ft_planning_stg_' + cast(@idSemana as varchar) + '.log'
							set @archivo_rsm = 'ft_planning_rsm_' + cast(@idSemana as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'SEMANA' as campo_ref, semana as valor
								from ##isk_rsm_planning
								where id_semana = -1
								union all
								select id_stage, 'COD_CLIENTE' as campo_ref, cast(cliente as varchar)
								from ##isk_rsm_planning
								where id_cliente = -1
								union all
								select id_stage, 'COD_EMPRESA', cast(empresa as varchar)
								from ##isk_rsm_planning
								where id_empresa = -1
								union all
								select id_stage, 'COD_PERFIL', tipo_categ
								from ##isk_rsm_planning
								where id_perfil = -1
								union all
								select id_stage, 'COD_TIPO_SERV_DETALLE', '<Calculado>'
								from ##isk_rsm_planning
								where id_tipo_serv_detalle <= 0) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_PLANNING b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_planning a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_PLANNING', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_planning order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_planning', @sql, @archivo_rsm
											
						end
				end

				-----------------------------------------------
				-- 'FT_MARKET_SHARE'
				-----------------------------------------------
				if @proceso = 'FT_MARKET_SHARE'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_market_share')
						drop table ##isk_rsm_market_share

					select a.id_stage,
						a.id_mes,
						a.id_empresa,
						a.id_sucursal,
						b.codigo_sucursal as cod_sucursal
					into ##isk_rsm_market_share
					from stg.PD_FT_MARKET_SHARE a,
						stg.ST_FT_MARKET_SHARE b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (id_sucursal = -1 )

					if @@rowcount > 0 
						begin
							-- Datos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_market_share_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_market_share_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'COD_SUCURSAL' as campo_ref, cod_sucursal as valor
								from ##isk_rsm_market_share
								where id_sucursal = -1
								union all
								select id_stage, 'COD_EMPRESA', '<Calculado>'
								from ##isk_rsm_market_share
								where id_empresa = -1
								) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_MARKET_SHARE b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_market_share a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_MARKET_SHARE', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_market_share order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_market_share', @sql, @archivo_rsm
											
						end
				end

				-----------------------------------------------
				-- 'FT_ESTADO_GESTION'
				-----------------------------------------------
				if @proceso = 'FT_ESTADO_GESTION'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_estado_gestion')
						drop table ##isk_rsm_estado_gestion

					select  a.id_stage, a.id_mes, a.id_sucursal, b.cod_sucursal, 
						a.id_linea, b.cod_linea_gestion, 
						b.tipo
					into ##isk_rsm_estado_gestion
					from stg.PD_FT_ESTADO_GESTION a,
						stg.ST_FT_ESTADO_GESTION b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (id_sucursal = -1
							or id_linea = -1)

					if @@rowcount > 0 
						begin
							-- Archivos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_estado_gestion_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_estado_gestion_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'COD_SUCURSAL' as campo_ref, cod_sucursal as valor
								from ##isk_rsm_estado_gestion
								where id_sucursal = -1
								union all
								select id_stage, 'COD_LINEA_GESTION', cod_linea_gestion
								from ##isk_rsm_estado_gestion
								where id_linea = -1
								) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_ESTADO_GESTION b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_estado_gestion a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_ESTADO_GESTION', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_estado_gestion order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_estado_gestion', @sql, @archivo_rsm
											
						end
				end

				-----------------------------------------------
				-- 'FT_COTIZACIONES'
				-----------------------------------------------
				if @proceso = 'FT_COTIZACIONES'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_cotizaciones')
						drop table ##isk_rsm_cotizaciones

					select  a.id_stage, a.nro_cotizacion,
						a.id_sucursal, b.codigo_sucursal as cod_sucursal,
						a.id_subcliente, b.codigo_subcliente as cod_subcliente, b.codigo_empresa as cod_empresa,
						a.id_cliente, b.codigo_cliente as cod_cliente, 
						a.id_tipo_serv_detalle, b.codigo_tipo_serv as cod_tipo_serv_detalle,
						a.id_tipo_cotiz, b.tipo_cotizacion as cod_tipo_cotiz,
						a.id_term_pago, b.codigo_termino_pago as cod_term_pago,
						a.id_status_cotiz, b.status_cot_codigo as cod_status_cotiz,
						a.id_contrato_ai, b.contrato_ai_codigo as cod_contrato_ai,
						a.id_fecha_activacion, b.fecha_activacion,
						a.id_fecha_creacion, b.fecha_creacion
					into ##isk_rsm_cotizaciones
					from stg.PD_FT_COTIZACIONES a,
						stg.ST_FT_COTIZACIONES b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (id_sucursal = -1
							or id_subcliente = -1
							or id_cliente = -1
							or id_tipo_serv_detalle = -1
							or id_tipo_cotiz = -1
							or id_term_pago = -1
							or id_status_cotiz = -1
							or id_contrato_ai = -1
							or id_fecha_activacion = 0
							or id_fecha_creacion = 0)
					order by 1

					if @@rowcount > 0 
						begin
							-- Archivos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_cotizaciones_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_cotizaciones_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'COD_SUCURSAL' as campo_ref, cod_sucursal as valor
								from ##isk_rsm_cotizaciones
								where id_sucursal = -1
								union all
								select id_stage, 'COD_SUBCLIENTE', cod_subcliente
								from ##isk_rsm_cotizaciones
								where id_subcliente = -1
								union all
								select id_stage, 'COD_CLIENTE', cod_cliente
								from ##isk_rsm_cotizaciones
								where id_cliente = -1
								union all
								select id_stage, 'COD_TIPO_SERV_DETALLE', cod_tipo_serv_detalle
								from ##isk_rsm_cotizaciones
								where id_tipo_serv_detalle = -1
								union all
								select id_stage, 'COD_TIPO_COTIZ', cod_tipo_cotiz
								from ##isk_rsm_cotizaciones
								where id_tipo_cotiz = -1
								union all
								select id_stage, 'COD_TERM_PAGO', cod_term_pago
								from ##isk_rsm_cotizaciones
								where id_term_pago = -1
								union all
								select id_stage, 'COD_STATUS_COTIZ', cod_status_cotiz
								from ##isk_rsm_cotizaciones
								where id_status_cotiz = -1
								union all
								select id_stage, 'COD_CONTRATO_AI', cod_contrato_ai
								from ##isk_rsm_cotizaciones
								where id_contrato_ai = -1
								union all
								select id_stage, 'FECHA_ACTIVACION', convert(varchar, isnull(fecha_activacion,'01/01/1900'), 103)
								from ##isk_rsm_cotizaciones
								where id_fecha_activacion <= 0
								union all
								select id_stage, 'FECHA_CREACION', convert(varchar, isnull(fecha_creacion,'01/01/1900'), 103)
								from ##isk_rsm_cotizaciones
								where id_fecha_creacion <= 0
								) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_COTIZACIONES b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_cotizaciones a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_COTIZACIONES', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_cotizaciones order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_cotizaciones', @sql, @archivo_rsm
											
						end
				end
			
				-----------------------------------------------
				-- 'FT_FACTURACION_TS'
				-----------------------------------------------
				if @proceso = 'FT_FACTURACION_TS'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_facturacion_ts')
						drop table ##isk_rsm_facturacion_ts

					select a.id_stage, 
						a.id_mes,
						a.id_empresa, 
						a.id_subcliente, b.codigo_subcliente as cod_subcliente,
						a.id_sucursal, isnull(b.codigo_sucursal,'<null>') as cod_sucursal,
						a.id_condic_subclte, b.condicion_cliente,
						a.id_tipo_serv_detalle, b.tipo_serv as cod_tipo_serv_detalle,
						a.id_cliente, b.customer_id as cod_customer
					into ##isk_rsm_facturacion_ts
					from stg.PD_FT_FACTURACION_TS a,
						stg.ST_FT_FACTURACION_TS b
					where a.id_stage = b.id_stage
						--and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (a.id_sucursal <= 0
							or id_condic_subclte = -1
							or id_subcliente = -1
							or id_tipo_serv_detalle = -1
							or id_cliente = -1)
						and id_condic_subclte > 0

					if @@rowcount > 0 
						begin
							-- Archivos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_facturacion_ts_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_facturacion_ts_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control) 
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'COD_SUCURSAL' as campo_ref, cod_sucursal as valor
								from ##isk_rsm_facturacion_ts
								where id_sucursal <= 0
								union all
								select id_stage, 'COD_SUBCLIENTE', cod_subcliente
								from ##isk_rsm_facturacion_ts
								where id_subcliente = -1
								union all
								select id_stage, 'COD_CLIENTE', cod_customer
								from ##isk_rsm_facturacion_ts
								where id_cliente = -1
								union all
								select id_stage, 'COD_TIPO_SERV_DETALLE', cod_tipo_serv_detalle
								from ##isk_rsm_facturacion_ts
								where id_tipo_serv_detalle = -1
								union all
								select id_stage, 'COD_CONDICION_CLIENTE', condicion_cliente
								from ##isk_rsm_facturacion_ts
								where id_condic_subclte = -1
								) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_FACTURACION_TS b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_facturacion_ts a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_FACTURACION_TS', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_facturacion_ts order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_facturacion_ts', @sql, @archivo_rsm
											
						end
				end

				-----------------------------------------------
				-- 'FT_PMC'
				-----------------------------------------------
				if @proceso = 'FT_PMC'
				begin

					if exists (select 1 from tempdb..sysobjects where name like '##isk_rsm_pmc')
						drop table ##isk_rsm_pmc

					select a.id_stage, 
						a.id_mes, a.id_dia,
						a.id_sucursal, b.cod_sucursal,
						a.id_subcliente, b.cod_subcliente,
						a.id_empresa, b.cod_empresa,
						a.id_cliente, b.cod_cliente
					into ##isk_rsm_pmc
					from stg.PD_FT_PMC a,
						stg.ST_FT_PMC b
					where a.id_stage = b.id_stage
						and (b.fecha_carga = convert(varchar, getdate(), 103) or @actual = 0)
						and (id_sucursal = -1
							or id_subcliente = -1
							or id_cliente = -1
							or id_empresa = -1)

					if @@rowcount > 0 
						begin
							-- Archivos
							set @registroCtrlProceso = @proceso + '-' + cast(@idMes as varchar)
							set @archivo_stg = 'ft_pmc_stg_' + cast(@idMes as varchar) + '.log'
							set @archivo_rsm = 'ft_pmc_rsm_' + cast(@idMes as varchar) + '.log'

							-- Registrar Control
							select @idControl = max(id_control)
							from etl.prc_ctrl_datos
							where registro_ctrl_proceso = @registroCtrlProceso

							if isnull(@idControl,0) > 0
								begin
									update etl.prc_ctrl_datos
									set fecha_control = getdate(),
										id_batch = @idBatch
									where id_Control = @idControl

									delete from etl.prc_ctrl_sks
									where id_Control = @idControl
								end						
							else
								begin
									insert into etl.prc_ctrl_datos (
										tipo_control, descripcion, id_batch, 
										registro_ctrl_proceso, archivo_stg, archivo_rsm)
									values ('ISK', 'Inconsistencias SKs en ' + @proceso, @idBatch, 
										@registroCtrlProceso, @archivo_stg, @archivo_rsm)

									set @idControl = SCOPE_IDENTITY()
								end

							-- Registrar Valores (Seed)
							insert into etl.prc_ctrl_sks (
								id_control, parametro, valor, cantidad)
							select @idControl, campo_ref, valor, count(1) as cantidad
							from (
								select id_stage, 'COD_SUCURSAL' as campo_ref, cod_sucursal as valor
								from ##isk_rsm_pmc
								where id_sucursal = -1
								union all
								select id_stage, 'COD_SUBCLIENTE', cod_subcliente
								from ##isk_rsm_pmc
								where id_subcliente = -1
								union all
								select id_stage, 'COD_CLIENTE', cod_cliente
								from ##isk_rsm_pmc
								where id_cliente = -1
								union all
								select id_stage, 'COD_EMPRESA', cod_empresa
								from ##isk_rsm_pmc
								where id_empresa = -1
								) a
							group by campo_ref, valor

							-- Exportar Stage (STG)
							set @sql = 'select * from stg.ST_FT_PMC b '
							set @sql = @sql + 'where exists (select 1 from ##isk_rsm_pmc a where a.id_stage = b.id_stage)'
							exec etl.prc_exportar_isk 'ST_FT_PMC', @sql, @archivo_stg

							-- Exportar Resumen (RSM)
							set @sql = 'select * from ##isk_rsm_pmc order by id_stage'
							exec etl.prc_exportar_isk '##isk_rsm_pmc', @sql, @archivo_rsm
											
						end
				end


				-- next
				fetch next from lc_cursor into @idProceso, @Proceso, @idBatch
			end
		close lc_cursor
		deallocate lc_cursor
	end try

	begin catch
		if xact_state() <> 0
			rollback

		exec etl.prc_log_error_sp 'CTROL-ISK', 0	
	end catch
	-- etl.prc_run_control_isk '2014-04-09', 0
end

GO
/****** Object:  StoredProcedure [etl].[prc_run_paquete_dts]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [etl].[prc_run_paquete_dts] (
	@idBatch numeric(15),
	@paqueteDTS varchar(500),
	@paramsDTS varchar(1000)=null)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 19-03-2014
	-- Description:	Correr Paquete DTS
	-- =============================================
	-- 23/03/2014 - Version Inicial
	-- 25/04/2018 - LS - Parametriza Dir DTS
	-- 16/01/2019 - LS - Amplia Paramatros de Ejecucion de DTS
	-- 14/01/2022 - LS - Migracion Azure - Contempla ejecucion en SSIS

	begin try

		declare @SQLQuery as varchar(2000)
		Declare @msg varchar(500), @sServerName varchar(100),
			@sDirPaquete varchar(255), @sDirLog varchar(500), @sOrigen varchar(20),
			@sArchivoLog varchar(255), @sql varchar(500), @sParam varchar(500),
			@rtn int, @param_log varchar(500), @parametros varchar(1000)

		if @paqueteDTS is null or ltrim(rtrim(@paqueteDTS)) = ''
			begin
				exec etl.prc_log_batch @idBatch, '** ERROR: Paquete DTS No Encontrado.'
				return -1
			end

		if @paramsDTS is null or @paramsDTS = '' 
			begin
				set @param_log = 'RUN-DTS-' + etl.fn_getClasificacionProceso(@idBatch)
				if @param_log is null
					set @parametros = etl.fn_getParametroGral('RUN-DTS-DWH')
				else
					set @parametros = etl.fn_getParametroGral(@param_log)
			end
		else
			begin
				set @parametros = @paramsDTS
			end

		select * 
		into #params
		from etl.fn_getTablaParametro(@parametros)

		if @@rowCount <= 0
			begin
				exec etl.prc_log_batch @idBatch, '** ERROR: Parametros de Ejecucion de Paquete DTS.'
				return -1
			end

		set @sArchivoLog = etl.fn_getNombreArchivoLog( @idBatch, 'DTS' )

		select @sOrigen = valor from #params where parametro = 'ORIGEN'
		select @sDirPaquete = valor from #params where parametro = 'DIR'
		select @sParam = valor from #params where parametro = 'PARAMS'
		if @sParam is null 
			set @sParam = '/MAXCONCURRENT " -1 " /CHECKPOINTING OFF /REPORTING EW'

		set @msg = 'Paquete DTS: ' + isnull(@paqueteDTS,'<>') + ' ('+ @sDirPaquete+') - SRC: ' + isnull(@sOrigen,'<null>') 
		exec etl.prc_log_batch @idBatch, @msg

		set @msg = 'Archivo Log: ' + @sArchivoLog
		exec etl.prc_log_batch @idBatch, @msg

								
		--set @SQLQuery = 'dtexec /DTS "'+@sDirPaquete+@paqueteDTS+'" /SERVER '+@sServerName+' /MAXCONCURRENT " -1 " /CHECKPOINTING OFF  /REPORTING E '
		--set @SQLQuery = 'dtexec /DTS "'+@sDirPaquete+@paqueteDTS+'" /MAXCONCURRENT " -1 " /CHECKPOINTING OFF  /REPORTING EW '


		if @sOrigen = 'SSISDB'
			begin
				set @paqueteDTS = @paqueteDTS + '.dtsx'
				--set @SQLQuery = 'dtexec /ISSERVER "'+@sDirPaquete+@paqueteDTS+'" /SERVER "localhost" /Par "$ServerOption::SYNCHRONIZED(Boolean)";True /X86 /MAXCONCURRENT " -1 " /CHECKPOINTING OFF  /REPORTING EW'
				set @SQLQuery = 'dtexec /ISSERVER "'+@sDirPaquete+@paqueteDTS+'" /SERVER "localhost" '+ @sParam 
			end
		else
			begin
				if @sOrigen = 'FILE' or substring(@sOrigen,1,1) = 'F'
					set @paqueteDTS = @paqueteDTS + '.dtsx'

				set @SQLQuery = 'dtexec /'+@sOrigen+' "'+@sDirPaquete+@paqueteDTS+'" '+ @sParam 
			end

		print @SQLQuery
		set @SQLQuery = @SQLQuery + '>> '+ @sArchivoLog
		exec @rtn = xp_cmdshell @SQLQuery, no_output

		If @rtn = 0 
			begin
				exec etl.prc_log_batch @idBatch, 'Ejecucion DTS: OK.'
				return 1
			end
		else
			begin
				set @msg = '** ERROR en la Ejecucion del Paquete DTS. (DTExec Rtn: ' + cast(@rtn as varchar) + ')'
				exec etl.prc_log_batch @idBatch, @msg 
				return -1
			end
	end try

	begin catch
		if xact_state() <> 0
			rollback transaction

		PRINT '** ERROR AL EJECUTAR PAQUETE: ' + ERROR_MESSAGE()

		-- ERROR
		return -1
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_run_proceso]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_run_proceso]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_run_proceso] (
	@idProceso numeric(15),
	@rtnMsg varchar(500) output,
	@idBatch numeric(15) output)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 14-03-2014
	-- Description:	Ejecucion de Proceso
	-- =============================================
	-- 26/03/2014 - Version Inicial
	-- 15/05/2014 - LS - Agrega Output del IdBatch
	-- 16/01/2019 - LS - Agrega Params DTS

	begin try

		declare @proceso varchar(100),
			@rtn int, 
			@paqueteDTS varchar(500),
			@paramsDTS varchar(1000)

		set @idBatch = 0

		-- Validar Si el Proceso esta Activo
		select @proceso = proceso,
			@paqueteDTS = paquete_dts,
			@paramsDTS = parametro_dts
		from etl.prc_proceso
		where id_proceso = @idProceso

		-- Iniciar Proceso (Get Id Batch)
		exec @rtn = etl.prc_iniciar_proceso @idProceso, @idBatch OutPut
		if @rtn <> 1 
			begin
				set @rtnMsg = 'Ocurrió un error al Iniciar Batch de Proceso.'
				return -1
			end

		-- Ejecutar Paquete
		exec @rtn = etl.prc_run_paquete_dts @idBatch, @paqueteDTS, @paramsDTS
		if @rtn <> 1 
			begin
				exec @rtn = etl.prc_finalizar_proceso @idBatch, 'ER'
				set @rtnMsg = 'Ocurrió un error en la Ejecucion del Paquete DTS.'
				return -1
			end 

		-- Finalizar Proceso
		exec @rtn = etl.prc_finalizar_proceso @idBatch, 'OK'
		if @rtn <> 1 
			begin
				exec @rtn = etl.prc_finalizar_proceso @idBatch, 'ER'
				set @rtnMsg = 'Ocurrió un error al Finalizar Batch de Proceso.'
				return -1
			end 

		-- OK
		set @rtnMsg = 'OK'
		return 1

	end try

	begin catch
		if xact_state() <> 0
			rollback tran

		set @rtnMsg = '** ERROR EN EJECUCION DE PROCESO: ' + isnull(ERROR_MESSAGE(),'')
		if isnull(@idBatch,0) > 0
			begin
				exec @rtn = etl.prc_finalizar_proceso @idBatch, 'ER'
				exec etl.prc_log_batch @idBatch, @rtnMsg
			end
		else
			set @idBatch = -1
		return -1
	end catch
end
GO
/****** Object:  StoredProcedure [etl].[prc_run_secuencia]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_run_secuencia]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_run_secuencia] (
	@idSecuencia numeric(15),
	@fechaProceso datetime)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 26-03-2014
	-- Description:	Ejecucion de Secuencia
	-- =============================================
	-- 26/03/2014 - Version Inicial
	-- 11/04/2014 - LS - Agregar Warning de proceso debido a Controles de datos
	-- 21/04/2014 - LS - Agrega Logica de Run x Verif. de Proceso
	-- 23/04/2014 - LS - Corrige: Join Query Cursor Procesos de la Secuencia
	-- 15/05/2014 - LS - Cambio de llamada a SP: prc_run_proceso para no perder @Idbatch + Ajuste
	-- 20/05/2014 - LS - Agrega Ejecucion AdHoc para un proceso de la secuencia
	-- 30/05/2014 - LS - Incluye Secuencia en Join de Verif. Dependiencia entre procesos
	-- 27/06/2014 - LS - Agregar en la Logica la Ejecucion a Demanda
	-- 07/07/2014 - LS - Registro de Periodo Actual de algunas tablas
	-- 08/07/2014 - LS - Agrega SP generador de periodos de DF (sacado por error)
	-- 08/08/2014 - LS - Ajuste en Update Periodo Actual + Ajustes Relacionados a la Demanda
	-- 11/08/2014 - LS - Agrega COnd. de Ejecucion de Control de Verif. segun la Exist. de una Demanda
	-- 25/08/2014 - LS - Cambio Orden en Relacion entre procesos y agrega nueva restriccion.
	-- 16/10/2015 - LS - Agrega hora de Ejecucion
	-- 19/09/2017 - LS - Order Nivel Ejecucion
	-- 03/01/2019 - LS - Exclusion para Procesos con Warning en Hora Ejecucion
	-- 17/10/2019 - LS - Ajuste para considerar multiples horas de ejecucion de un mismo proceso (INC7882100)
	-- 30/10/2019 - LS - Mas Ajuste por Nro de Ejecucion
	-- 21/11/2019 - LS - Mas Ajuste por Nro de Ejecucion (Orden al Tomar los procesos + Ajuste REstriccion)

	set nocount on

	begin try

		declare @msg varchar(500), @idBatchSeq numeric(15), 
			@rtn numeric(10), @reproceso bit, @idDemanda numeric(15),
			@estadoProceso char(2), @estadoSecuencia char(2),
			@prcOK int, @prcER int, @prcNO int, @prcWN int,
			@detError char(1), @detenerSeq bit, @run bit,
			@descWarning varchar(255), @runVerif bit, @Demanda bit,
			@idProceso numeric(15), @idBatch numeric(15),
			@idPeriodo varchar(100), @parametros varchar(500),
			@idPeriodoActual varchar(100), @mail bit, @nroEjecucion numeric(15)


		-- Fecha de Proceso
		set @fechaProceso = etl.fn_truncFecha( @fechaProceso )
		-------------------------------------------
		-- Armar Logica de Ejecucion
		-------------------------------------------
		exec @rtn = etl.prc_gen_batch_secuencia @idSecuencia, @fechaProceso, @msg output, @idBatchSeq output
		if isnull(@rtn,-1) = -1
			raiserror(@msg, 16, 1) WITH NOWAIT
		else
			if @rtn = 0
				return


		-- Reproceso
		select @reproceso = case reproceso when 'S' then 1 else 0 end
		from etl.prc_batch_secuencia
		where id_batch_seq = @idBatchSeq

		-- Genera Periodos de Verificacion de Procesos Validos
		-- (La Generacion es mensual, pero verif. constantemente aun si debe modificarse...)
		exec etl.prc_gen_verificacion_df @fechaProceso

		-- Actualiza el/los Parametros que toma cada proceso de la secuencia 
		-- para su ejecucion (Desde Paquete DTS)
		update PRC
		set parametro_seq = seq.parametro
		from etl.prc_proceso PRC,
			etl.prc_batch_secuencia_proceso seq
		where PRC.id_proceso = seq.id_proceso
			and id_batch_seq = @idBatchSeq
	
		-------------------------------------------
		-- Ejecucion de Procesos de Secuencia
		-------------------------------------------
		set @prcOK = 0
		set @prcER = 0
		set @prcWN = 0
		set @detenerSeq = 0
		set @Demanda = 0
		set @mail = 1

		declare lc_cursor_seq cursor for
		select id_proceso, detencion_error, run_verif, id_demanda, nro_ejecucion
		from (
			select a.id_proceso, isnull(b.detencion_error,'S') as detencion_error,
				a.run_verif, a.id_demanda,
				min(a.nivel_ejec) as nivel_ejec, 
				min(a.orden_ejec) as orden_ejec,
				min(nro_ejecucion) as nro_ejecucion
			from etl.prc_batch_secuencia_proceso a,
				etl.prc_secuencia_proceso b,
				etl.prc_batch_secuencia c
			where a.id_proceso = b.id_proceso
				and a.id_batch_seq = c.id_batch_seq
				and a.id_batch_seq = @idBatchSeq
				and b.id_secuencia = c.id_secuencia
				and (a.estado = 'IN' or (a.reproceso = 'S' and a.estado <> 'OK'))
				and (a.estado = 'IN' and (a.hora_ejecucion is null or 
							cast(replace(a.hora_ejecucion,':','') as numeric(10)) <= 
							cast(replace(substring(convert(varchar,getdate(),108),1,5),':','') as numeric(10)) ))
			group by a.id_proceso, isnull(b.detencion_error,'S'),
				a.run_verif, a.id_demanda) vw
		order by nro_ejecucion, nivel_ejec, orden_ejec
		open lc_cursor_seq
		fetch next from lc_cursor_seq into @idProceso, @detError, @runVerif, @idDemanda, @nroEjecucion
		while @@fetch_status = 0
			begin

				if @Demanda = 0 and @idDemanda > 0
					set @Demanda = 1
				
				-- Analizar Relaciones Existentes entre Procesos
				if exists (select 1
						from etl.prc_rel_secuencia_proceso
						where id_proceso_hijo = @idProceso
							and id_secuencia = @idSecuencia) 
					-- Verif. Dependencias con Procesos Padres
					if exists (
						select 1 
						from (select sum(ejec) ejec, sum(prc) prc
							from (
								select vw.id_proceso_padre, 
									vw.restriccion_error,
									b.estado, 
									case when b.id_proceso is null then 
										case restriccion_dependencia when 'S' then
											0
										else
											1
										end 
										when vw.restriccion_error = 'S' and b.estado <> 'OK' then 
											0
										else 
											1
										end ejec,
										1 prc
								from 
									(select distinct c.id_proceso_padre, c.id_proceso_hijo, c.restriccion_error,
										c.restriccion_dependencia, a.id_secuencia, a.id_batch_seq,
										@nroEjecucion as nro_ejecucion
									from etl.prc_rel_secuencia_proceso c,
										etl.prc_batch_secuencia a
									where 1 = 1 
										and a.id_secuencia = c.id_secuencia
										and a.id_batch_seq = @idBatchSeq
										and c.id_proceso_hijo = @idProceso) vw 
									left outer join etl.prc_batch_secuencia_proceso b
										on vw.id_batch_seq = b.id_batch_seq
										and vw.id_proceso_padre = b.id_proceso
										and vw.nro_ejecucion = b.nro_ejecucion
								) x ) y
						where ejec = prc)
						set @run = 1
					else
						set @run = 0
				else
					set @run = 1	

				-- Run Proceso
				if @run = 1
					begin
						exec @rtn = etl.prc_run_proceso @idProceso, @msg output, @idBatch output
						if isnull(@rtn,0) <> 1
							begin
								set @estadoProceso = 'ER'
								set @prcER = @prcER + 1
								set @msg = isnull(@msg, 'Error en la Ejecucion de Proceso.')
								if isnull(@idBatch,0) <= 0
									set @idBatch = -1
							end
						else
							begin
								-- Revisa si hay algun Control Activado en el Proceso (Warning)
								-- Si existe cambia el Estado
								select @descWarning = isnull(x.ref,'<>')
								from (
									select top 1  'Ref. Id Ctrl.: ' + substring(cast(id_control + 100000000 as varchar),2,8) + ')' ref
									from etl.prc_ctrl_datos
									where etl.fn_truncFecha(getdate()) = etl.fn_truncFecha( fecha_control )
										and tipo_control in ('MAP')
										and id_batch = @idBatch) x

								-- Warning
								if @@rowcount > 0
									begin
										set @estadoProceso = 'WN'
										set @prcWN = @prcWN + 1
										set @msg = 'Proceso Ejecutado con WARNING (' + @descWarning + ')'
									end
								else
									begin
										set @estadoProceso = 'OK'
										set @prcOK = @prcOK + 1
										set @msg = 'Proceso Ejecutado Correctamente.'

										-- Actualiza Ult Proceso OK
										update etl.prc_proceso
										set ult_proceso_ok = case 
													when @fechaProceso >= isnull(ult_proceso_ok, @fechaProceso) then @fechaProceso
													else ult_proceso_ok 
												end, 
											ult_id_batch_ok = case 
													when @idBatch >= isnull(ult_id_batch_ok, @idBatch) then @idBatch
													else ult_id_batch_ok
												end
										where id_proceso = @idProceso

										-- Obtener Periodo de Corte del Proceso (Del parametro)
										select @idPeriodo = etl.fn_getValorParametro (prc.parametros, prc.id_periodo),
											@idPeriodoActual = etl.fn_getValorParametro (actual.parametro_act, prc.id_periodo)
										from (select case pr.periodicidad 
													when 'M' then 'idMes'
													when 'S' then 'idSemana'
													when 'D' then 'idDia'
													else '' end as id_periodo,
													bt.parametros
												from etl.prc_proceso pr,
													etl.prc_proceso_batch bt
												where pr.id_proceso = bt.id_proceso
													and bt.id_batch = @idBatch ) prc,
											(select etl.fn_getParamProcesoFecha (@idProceso,
												@fechaProceso, default) as parametro_act) actual

										-- Registra Periodo Actual en Algunos Procesos
										if @idPeriodo is not null and exists( select 1 from etl.prc_periodo_actual_proceso where id_proceso = @idProceso)
											begin
												if @idPeriodo is not null
													update etl.prc_periodo_actual_proceso
													set id_periodo = @idPeriodo,
														fecha_registro = getdate()
													where id_proceso = @idProceso
														and (id_periodo < @idPeriodo or id_periodo is null)
											end

									end
							end
					end
				else
					begin
						set @estadoProceso = 'NO'
						set @prcNO = @prcNO + 1
						set @idBatch = 0
						set @msg = 'Proceso No Ejecutado debido a Restriccion entre Relacion entre Procesos.'
					end

				-- Controlar Verificacion de Datos Fuentes
				if isnull(@idDemanda,0) <= 0 or (
						isnull(@idDemanda,0) > 0 and (@idPeriodo >= @idPeriodoActual or isnull(@idPeriodoActual,'') = '')
						)
					exec etl.prc_ctrl_verificacion_df @idProceso, @fechaProceso,
						@run, @runVerif, @estadoProceso
					
				update etl.prc_batch_secuencia_proceso
				set fecha_final = getdate(),
					id_batch = isnull(@idBatch,0),
					estado = @estadoProceso,
					mensaje = @msg,
					run_verif = @runVerif
				where id_batch_seq = @idBatchSeq
					and id_proceso = @idProceso
					and nro_ejecucion = @nroEjecucion

				update DMD 
				set estado = @estadoProceso,
					fecha_estado = getdate(),
					detalles = @msg
				from etl.prc_demanda_ejecucion DMD,
					etl.prc_batch_secuencia_proceso bsp
				where DMD.id_demanda = bsp.id_demanda
					and DMD.id_proceso = bsp.id_proceso
					and bsp.id_batch_seq = @idBatchSeq
					and bsp.nro_ejecucion = @nroEjecucion
					
				-- Detencion de Secuencua
				if @detError = 'S' and @estadoProceso = 'ER'
					begin
						set @detenerSeq = 1

						update etl.prc_batch_secuencia_proceso
						set fecha_final = getdate(),
							id_batch = 0,
							estado = 'NO',
							mensaje = 'Proceso No Ejecutados debido a detencion de la secuencia.'
						where id_batch_seq = @idBatchSeq
							and estado = 'IN'
						break
					end

				-- next
				fetch next from lc_cursor_seq into @idProceso, @detError, @runVerif, @idDemanda, @nroEjecucion
			end
		close lc_cursor_seq
		deallocate lc_cursor_seq

		-- Determinar Estado de la Secuencia
		if @detenerSeq = 1 
			begin
				set @msg = '** ERROR: No es posible continuar con la Secuencia debido a la ejecucion de un Proceso. (ID: ' + cast(@idProceso as varchar)+ ')' 
				set @estadoSecuencia = 'ER'					
			end
		else
			if (@prcNO > 0 or @prcWN > 0) and @prcER = 0
				begin
					set @estadoSecuencia = 'WN'
					if @prcWN > 0
						set @msg = '** WARNING: Existen procesos de la secuencia ('+ cast(@prcWN as varchar)+') en estado de advertencia. (Q: ' + cast(@prcOK as varchar)+ ')'
					else 
						set @msg = '** WARNING: Existen procesos de la secuencia ('+ cast(@prcNO as varchar)+') que no se han ejecutado. (Q: ' + cast(@prcOK as varchar)+ ')'
				end
			else
				if @prcOK = 0 and @prcER = 0
					begin
						if @idSecuencia not in (3 , 102, 103)
							begin
								set @msg = '** WARNING: No se ha ejecutado ningún proceso de la secuencia.'
								set @estadoSecuencia = 'WN'
							end
						else
							begin
								set @mail = 0
								set @msg = 'No se ha ejecutado ningún proceso de la secuencia.'
								set @estadoSecuencia = 'IN'
							end
					end
				else
					if @prcOK = 0 and @prcER > 0
						begin
							set @msg = '** ERROR: Todos los procesos de la secuencia fallaron. (Q: ' + cast(@prcER as varchar)+ ')' 
							set @estadoSecuencia = 'ER'					
						end
					else
						if @prcOK > 0 and @prcER > 0
							begin
								set @msg = '** WARNING: Algunos procesos de la secuencia fallaron. (Q: ' + cast(@prcER as varchar)+ ')' 
								set @estadoSecuencia = 'WN'					
							end
						else
							begin
								set @msg = 'OK: Los procesos de la secuencia se ejecutaron correctamente. (Q: ' + cast(@prcOK as varchar)+ ')' 
								set @estadoSecuencia = 'OK'					
							end



		-- Finalizar Secuencia
		update etl.prc_batch_secuencia
		set estado = @estadoSecuencia,
			fecha_final = getdate(),
			mensaje = @msg
		where id_batch_seq = @idBatchSeq

		-- Enviar Mail
		if @mail = 1
			begin
				if @Demanda = 1
					exec etl.prc_enviar_mail_secuencia @idSecuencia, @fechaProceso, 0
				else
					exec etl.prc_enviar_mail_secuencia @idSecuencia, @fechaProceso, @reproceso
			end

	end try

	-- Captura de Error
	begin catch
		set @msg = '** ERROR: "' + isnull(ERROR_MESSAGE(),'')
	
		update etl.prc_batch_secuencia
		set estado = 'ER',
			fecha_final = getdate(),
			mensaje = @msg
		where id_batch_seq = @idBatchSeq

		exec etl.prc_log_error_seq @msg	

		-- Enviar Mail
		if @Demanda = 1
			exec etl.prc_enviar_mail_secuencia @idSecuencia, @fechaProceso, 0
		else
			exec etl.prc_enviar_mail_secuencia @idSecuencia, @fechaProceso, @reproceso


	end catch
	-- exec etl.prc_run_secuencia 3, '2015-10-16'
	-- rollback transaction
end
GO
/****** Object:  StoredProcedure [etl].[prc_VerificarEjecucionJob]    Script Date: 17/03/2022 12:40:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [etl].[prc_VerificarEjecucionJob]    Script Date: 20/12/2021 05:47:08 p.m. ******/
CREATE procedure [etl].[prc_VerificarEjecucionJob] (
	@JobsPredecesores varchar(1000),
	@minutosVerif int = 240,
	@intentos int = 50)
as
begin

	-- ==========================================================================================
	-- Author:		Luciano Silvera
	-- Create date: 29-05-2014
	-- Description:	Verificar si se Ejecuta un JOB
	-- ==========================================================================================
	-- 29/05/2014 - Version Inicial

	declare @nroIntentos int,
		@final datetime,
		@activo int,
		@rtn bit

	set @rtn = 0
	set @nroIntentos = 0
	set @final =  dateadd (minute, @minutosVerif , getdate())
	while @nroIntentos < @intentos and getdate() < @final
		begin

			select @activo = sum(case when etl.fn_getJobActivo(valor) = 1 then 1 else 0 end )
			from etl.fn_getTablaLista ( @JobsPredecesores )

			if @activo = 0 
				begin
					-- Ejecucion.....
					set @rtn =  1
					break
				end 
			else
				begin
					-- Esperar 5 Minutos y volver a intentar
					set @nroIntentos = @nroIntentos + 1
					WAITFOR DELAY '00:05:00'
				end 
		end

	return @rtn
end
GO
