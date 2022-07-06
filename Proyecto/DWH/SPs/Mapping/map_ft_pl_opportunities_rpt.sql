use MX_DWH
Go

if object_id ('etl.map_ft_pl_opportunities_rpt', 'P') is not null
	drop procedure etl.map_ft_pl_opportunities_rpt
GO

create procedure etl.map_ft_pl_opportunities_rpt (
	@idBatch numeric(15),
	@fechaProceso datetime)
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	10-06-2022
	-- Description:	Oportunidades SF (Reports) - Foto Semanal
	-- =============================================
	-- 10/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@idSemana numeric(10), @fecDesde datetime, @fecHasta datetime, 
			@fechaSemana datetime

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'Fecha Proceso: ' + format(@fechaProceso, 'dd/MM/yyyy'); exec etl.prc_Logging @idBatch, @msg

		-- Get Info
		set @fechaSemana = @fechaProceso - 1

		select @idSemana = a.id_semana, 
			@fecDesde = semana_desde, 
			@fecHasta = semana_hasta
		from dbo.DTPO_LK_SEMANAS a
		where @fechaSemana between a.semana_desde and a.semana_hasta

		set @msg = 'Fecha Semana: ' + format(@fechaSemana, 'dd/MM/yyyy'); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'Semana: ' + cast(@idSemana as varchar) + ' (' + format(@fecDesde, 'dd/MM/yyyy') +' - '+format(@fecHasta, 'dd/MM/yyyy')+')'
		exec etl.prc_Logging @idBatch, @msg

		begin transaction

			-- Borrados
			delete from dbo.FT_PL_OPP_PIPELINE
			where id_semana = @idSemana

			set @msg = 'Del. (Pipeline): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			delete from dbo.FT_PL_OPP_VENCIDAS
			where id_semana = @idSemana

			set @msg = 'Del. (Opps Vencidas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			delete from dbo.FT_PL_OPP_PERDIDAS
			where id_semana = @idSemana

			set @msg = 'Del. (Opps Perdidas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			delete from dbo.FT_PL_OPP_GANADAS
			where id_semana = @idSemana

			set @msg = 'Del. (Opps Ganadas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			delete from dbo.FT_PL_OPP_NUEVAS
			where id_semana = @idSemana

			set @msg = 'Del. (Opps Nuevas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			delete from dbo.FT_PL_OPP_EMBUDO
			where id_semana = @idSemana

			set @msg = 'Del. (Embudo): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg
			

			-- (1) Pipeline Abierto 
			insert into dbo.FT_PL_OPP_PIPELINE (id_semana, fecha_proceso, 
				opportunityid, pais, fecha_cierre, propietario, 
				cantidad_opp, antiguedad, moneda, importe, importe_mensual, mb_previsto, 
				nombre_cuenta, subservicio, servicio, segmento_local)	
			select @idSemana, @fechaProceso,
				OpportunityID, Pais, Fecha_Cierre, 
				ft.Propietario_Opp as Propietario, Cantidad_Opp, 
				datediff(day, ft.fecha_creacion, @fechaProceso ) as Antiguedad,
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Nombre_Cuenta, ft.Subservicio, ft.Servicio, ft.Segmento_Local
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso
				and ft.fecha_cierre >= @fechaProceso
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and ft.etapa_opp in ('Calificar', 'Desarrollar', 'Proponer', 'Negociación Final')

			set @msg = 'Ins. (Pipeline): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			-- (2) Opps Vencidas
			insert into dbo.FT_PL_OPP_VENCIDAS (id_semana, fecha_proceso,
				OpportunityID, Pais, Propietario, Nombre_Opp, Nombre_Cuenta, Fecha_Cierre, 
				Probabilidad, Antiguedad, Moneda, Importe, Importe_Mensual, MB_Previsto, 
				Sucursal, Servicio )
			select @idSemana, @fechaProceso,
				ft.OpportunityID, ft.Pais, 
				ft.Propietario_Opp as Propietario, 
				ft.Nombre_Opp,
				ft.Nombre_Cuenta,
				ft.Fecha_Cierre, 
				ft.Probabilidad, 
				datediff(day, ft.fecha_creacion, @fechaProceso ) as Antiguedad,
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Sucursal, ft.Servicio
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso 
				and datediff(year, ft.fecha_creacion , @fechaProceso ) in (0,1)
				and ft.fecha_cierre < @fecDesde -- Menor q Semana Pasada
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and ft.etapa_opp in ('Proponer')
				and ft.probabilidad not in (99, 100)

			set @msg = 'Ins. (Opps Vencidas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg


			-- (3) Opps Perdidas
			insert into dbo.FT_PL_OPP_PERDIDAS (id_semana, fecha_proceso,
				OpportunityID, Pais, Nombre_Opp, Nombre_Cuenta, Fecha_Cierre, Moneda, Importe, 
				Importe_Mensual, MB_Previsto, Servicio)
			select @idSemana, @fechaProceso,
				ft.OpportunityID, 
				ft.Pais, 
				ft.Nombre_Opp,
				ft.Nombre_Cuenta,
				ft.Fecha_Cierre, 
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Servicio
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso 
				and ft.ult_modif_stage between @fecDesde and @fecHasta
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and ft.etapa_opp in ('Cerrado Perdido', 'Cerrado No Calificado', 'Closed no Bid')
				and ft.propuesta_enviada = 1
				and isnull(ft.razon_gp,'') != 'Administrative mistake' 
				and isnull(ft.razon_gp_comment,'') not like '%Error%Administrativo%'
				and isnull(ft.razon_gp_comment,'') not like '%Error%Adm%'
				and ft.servicio in ('Outsourcing', 'Payroll', 'Permanent placement', 'RPO', 'Temporary staffing', 'Training')

			set @msg = 'Ins. (Opps Perdidas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			-- (4) Opps Ganadas
			insert into dbo.FT_PL_OPP_GANADAS (id_semana, fecha_proceso,
				OpportunityID, Pais, Nombre_Opp, Nombre_Cuenta, Fecha_Cierre, Moneda, Importe, 
				Importe_Mensual, MB_Previsto, Servicio)
			select @idSemana, @fechaProceso,
				ft.OpportunityID, 
				ft.Pais, 
				ft.Nombre_Opp,
				ft.Nombre_Cuenta,
				ft.Fecha_Cierre, 
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Servicio
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso 
				and ft.ult_modif_stage between @fecDesde and @fecHasta
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and ft.etapa_opp in ('Cerrado Ganado', 'Closed Won (not signed)')
				and ft.probabilidad = 100
				and ft.servicio in ('Outsourcing', 'Payroll', 'Permanent placement', 'RPO', 'Temporary staffing', 'Training')

			set @msg = 'Ins. (Opps Ganadas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			-- (5) Nuevas Opps
			insert into dbo.FT_PL_OPP_NUEVAS (id_semana, fecha_proceso,
				OpportunityID, Pais, Nombre_Opp, Nombre_Cuenta, Fecha_Cierre, Moneda, Importe, 
				Importe_Mensual, MB_Previsto, Servicio, Antiguedad)
			select @idSemana, @fechaProceso,
				ft.OpportunityID, 
				ft.Pais, 
				ft.Nombre_Opp,
				ft.Nombre_Cuenta,
				ft.Fecha_Cierre, 
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Servicio,
				datediff(day, ft.fecha_creacion, @fechaProceso ) as Antiguedad
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso 
				and (ft.fecha_creacion between @fecDesde and @fecHasta
					or ft.ult_modif_stage between @fecDesde and @fecHasta )
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and ft.etapa_opp in ('Calificar', 'Desarrollar', 'Proponer')
				and ft.servicio in ('Outsourcing', 'Payroll', 'Permanent placement', 'RPO', 'Temporary staffing', 'Training')

			set @msg = 'Ins. (Opps Nuevas): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

			-- (6) Embudo
			insert into dbo.FT_PL_OPP_EMBUDO (id_semana, fecha_proceso,
				OpportunityID, Pais, Nombre_Cuenta, Propietario, Servicio, Fecha_Cierre, 
				Cantidad_Opp, Antiguedad, Moneda, Importe, Importe_Mensual, MB_Previsto, 
				Subservicio, Tipo_Opp, Etapa_Opp)
			select @idSemana, @fechaProceso,
				ft.OpportunityID, 
				ft.Pais, 
				ft.Nombre_Cuenta,
				ft.Propietario_Opp as Propietario,
				ft.Servicio,
				ft.Fecha_Cierre, 
				ft.Cantidad_Opp, 
				datediff(day, ft.fecha_creacion, @fechaProceso ) as Antiguedad,
				case when ods.cod_moneda is not null then 'EUR' else ft.cod_moneda end as Moneda,
				ft.venta_potencial * isnull(ods.Eur_x_Loc,1) as Importe,
				ft.importe * isnull(ods.Eur_x_Loc,1) as Importe_Mensual,
				ft.MB_Previsto_Porc as MB_Previsto,
				ft.Subservicio,
				ft.Tipo_Opp,
				ft.Etapa_Opp
			from FT_PL_OPPORTUNITIES ft
				left outer join (select * from dbo.ODS_PL_OPP_BUDGET_RATE
						where año = year(@fechaSemana)) ods
						on ods.cod_moneda = ft.cod_moneda
			where 1=1
				and ft.fecha_creacion < @fechaProceso 
				and ft.tipo_opp in ('Nuevo Cliente', 'Cliente Existente: Nuevo Proyecto / Nuevo Alcance')
				and isnull(ft.razon_gp,'') != 'Administrative mistake' 
				and isnull(ft.razon_gp_comment,'') not like '%Error%Administrativo%'
				and isnull(ft.razon_gp_comment,'') not like '%Error%Adm%'
				and ( (ft.etapa_opp is not null 
						and ft.ult_modif_stage >= '2021-01-01') -- Consultar Criterio
					or 
					(ft.fecha_cierre >= @fechaProceso) 
						and ft.etapa_opp in ('Identificar', 'Calificar', 'Identificar/Calificar', 'Desarrollar', 'Proponer',
							'Negociación Final', 'Win Pending', 'Qualify_Modis', 'Develop_Modis', 'Consultant Interviews',
							'Propose_Modis', 'Negotiate') )

			set @msg = 'Ins. (Embudo): ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

		commit transaction


	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

		-- error
		return -1

	end catch
end
GO
-- exec etl.map_ft_pl_opportunities_rpt 0, '2022-07-04'