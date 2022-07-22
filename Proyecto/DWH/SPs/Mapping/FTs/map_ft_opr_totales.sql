use MX_DWH
Go

if object_id ('etl.map_ft_opr_totales', 'P') is not null
	drop procedure etl.map_ft_opr_totales
GO

create procedure etl.map_ft_opr_totales (
	@idBatch numeric(15),
	@idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 07/04/2022
	-- Description:	Mapping FT_OPR - Calculo Totales
	-- =============================================
	-- 14/07/2022 - Version Inicial
	-- 22/07/2022 - LS - Ajuste Allocation Branch
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10)
		declare @id_linea numeric(10), @id_linea2 numeric(10), 
			@desc_linea_n2 varchar(255), @calculo varchar(500),
			@total numeric(22,4), @parcial numeric(22,4), @parcial_budget numeric(22,4),
			@total_budget numeric(22,4)
		declare @nro int, @valor varchar(50), @tipo char(3),
			@operacion char(1)
		declare @id_mes numeric(10), @id_cc_orig numeric(10), @id_cc_opr numeric(10), 
			@id_cliente numeric(10), @id_subclte numeric(10), @id_empresa numeric(10)
		declare @inicio datetime, @contar int, @real_num numeric(22,4), @real_den numeric(22,4),
			@division numeric(1), @budget_num numeric(22,4), @budget_den numeric(22,4)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Mes/es
		set @msg = 'Id Mes: ' + convert(varchar, @idMes); exec etl.prc_Logging @idBatch, @msg

		-------------------------------------------
		-- PREPROCESAMIENTO DATOS (PD)
		-------------------------------------------
		select cast('FT' as varchar(20)) as origen,
			ft.id_mes,
			lk.id_linea2, 
			ft.id_cc_orig, 
			ft.id_cc_opr, 
			ft.id_cliente, 
			ft.id_subclte, 
			ft.id_empresa,
			sum(ft.real) as real,
			sum(ft.budget) as budget,
			cast(0 as numeric(22,4)) as real_num,
			cast(0 as numeric(22,4)) as real_den,
			cast(0 as numeric(22,4)) as budget_num,
			cast(0 as numeric(22,4)) as budget_den,
			0 as division,
			0 as flag
		into #total_ln2
		from dbo.FT_OPR_DETALLE_CUENTA ft,
			dbo.DESG_LK_LINEA lk
		where ft.id_linea = lk.id_linea
			and ft.id_mes = @idMes
			and lk.id_linea2 not in  (select distinct id_linea2 
						from dbo.DESG_LK_LINEA x
						where x.id_linea > 0
							and x.calculo is not null 
							and len(x.calculo) > 1)
			and ft.origen not in ('TOTAL')
		group by ft.id_mes,
			lk.id_linea2, 
			ft.id_cc_orig, 
			ft.id_cc_opr, 
			ft.id_cliente, 
			ft.id_subclte, 
			ft.id_empresa
		order by ft.id_mes, lk.id_linea2

		set @msg = 'Totales LN2 (CC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg	
		
		create unique index #total_ln2_uk on #total_ln2 (
			id_mes, id_linea2, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, flag)

		------------
		-- CALCULO
		-----------
		set @inicio = getdate()
		set @contar = 0
		exec etl.prc_Logging @idBatch, '- Inicio de Calculo: '

		begin
			declare lc_cursor cursor for
			select lk.id_linea, lk.id_linea2, lk.desc_linea_n2, replace(lk.calculo,' ', '') as calculo
			from dbo.DESG_LK_LINEA lk
			where lk.calculo is not null and len(lk.calculo) > 1
			order by cast(lk.id_linea2 as numeric)

			open lc_cursor
			fetch next from lc_cursor into @id_linea, @id_linea2, @desc_linea_n2, @calculo
			while (@@fetch_status = 0)
			begin

				set @division = 0
				if @calculo like '%/%'
					set @division = 1
					
				-- Print @calculo

				-- CTA CBLE / DETALLE
				declare lc_datos cursor for
				select distinct id_mes, id_cc_orig, id_cc_opr, id_cliente, id_subclte, id_empresa	
				from #total_ln2 t
				where origen = 'FT'

				open lc_datos
				fetch next from lc_datos into @id_mes, @id_cc_orig, @id_cc_opr, @id_cliente, @id_subclte, @id_empresa
				while (@@fetch_status = 0)
				begin
					set @contar = @contar + 1
					set @total = 0
					set @operacion = ''
					set @real_num = null
					set @real_den = null
					set @budget_num = null
					set @budget_den = null

					declare lc_cur_opp cursor for
					select nro, valor, tipo
					from etl.fn_getTablaOperaciones (@calculo)
					order by nro

					open lc_cur_opp
					fetch next from lc_cur_opp into @nro, @valor, @tipo
					while (@@fetch_status = 0)
					begin
						if @tipo = 'NUM'
							begin
								-- Si es Valor fijo
								if upper(substring(@valor,1,1)) = 'F'
									begin
										set @parcial = etl.fn_getNumeroValido(  replace(@valor, 'f', ''), '.' )
									end
								else
									begin
										select @parcial = sum(real), @parcial_budget = sum(budget)
										from #total_ln2
										where id_linea2 = cast(@valor as numeric)
											and id_mes = @id_mes
											and id_cc_orig = @id_cc_orig
											and id_cc_opr = @id_cc_opr
											and id_cliente = @id_cliente
											and id_subclte = @id_subclte
											and id_empresa = @id_empresa
									end

								if @parcial is null 
									set @parcial = 0
								if @parcial_budget is null
									set @parcial_budget = 0
								if @division = 1
									begin
										if @nro = 1
										begin
											set @real_num  = @parcial
											set @budget_num  = @parcial_budget
										end
										if @nro = 3
										begin
											set @real_den  = @parcial
											set @budget_den  = @parcial_budget
										end
									end
							end

						if @tipo = 'OPE'
							begin
								if @valor = '+'
									set @operacion = '+'
								if @valor = '-'
									set @operacion = '-'
								if @valor = '*'
									set @operacion = '*'
								if @valor = '/'
									set @operacion = '/'
							end

						if @tipo = 'NUM'
							begin
								if @operacion = ''
								begin
									set @total = @parcial
									set @total_budget = @parcial_budget
								end
								if @operacion = '+'
								begin
									set @total = @total + @parcial
									set @total_budget = @total_budget + @parcial_budget
								end
								if @operacion = '-'
								begin
									set @total = @total - @parcial
									set @total_budget = @total_budget - @parcial_budget
								end
								if @operacion = '*'
								begin
									set @total = @total * @parcial
									set @total_budget = @total_budget * @parcial_budget
								end
								if @operacion = '/' 
								begin
									if @parcial != 0
										set @total = cast( cast(@total as float) / cast(@parcial as float) as numeric(22,4))
									else
										set @total = 0
									if @parcial_budget != 0
										set @total_budget = cast( cast(@total_budget as float) / cast(@parcial_budget as float) as numeric(22,4))
									else
										set @total_budget = 0
								end
							end
						-- next
						fetch next from lc_cur_opp into @nro, @valor, @tipo
					end
					close lc_cur_opp
					deallocate lc_cur_opp

					insert into #total_ln2 (origen, id_linea2, real, budget,
						id_mes, id_cc_orig, id_cc_opr, id_cliente, id_subclte, id_empresa,
						real_num, real_den, budget_num, budget_den, division, flag)
					values ('CALC', @id_linea2, @total, @total_budget,
						@id_mes, @id_cc_orig, @id_cc_opr, @id_cliente, @id_subclte, @id_empresa,
						@real_num, @real_den, @budget_num, @budget_den, @division, 0)

					-- next
					fetch next from lc_datos into @id_mes, @id_cc_orig, @id_cc_opr, @id_cliente, @id_subclte, @id_empresa

				end
				close lc_datos
				deallocate lc_datos
		
				-- next
				fetch next from lc_cursor into @id_linea, @id_linea2, @desc_linea_n2, @calculo
			end
			close lc_cursor
			deallocate lc_cursor

			set @msg = 'Registros Procesados: ' + convert(varchar, @contar); exec etl.prc_Logging @idBatch, @msg	
			set @msg = '- Fin del Calculo: ' + etl.fn_tiempo(@inicio, getdate()); exec etl.prc_Logging @idBatch, @msg

		
			-- Excepcion Total Allocation Branch
			-- CCs Agrup: 102980,702018,910500,910650,910660,910670,803000,101900,910620
			
			-- Borrar Calculos Total Allocation Branch
			delete st
			from #total_ln2 st,
				dbo.VW_LK_CC_OPR_HIST cc,
				(select * from dbo.DESG_LK_LINEA where cod_cuenta = '990350') ln,
				dbo.DEMP_LK_EMPRESA em
			where st.id_linea2 = ln.id_linea2
				and st.id_mes = cc.id_mes
				and st.id_cc_opr = cc.id_cc
				and st.id_empresa = em.id_empresa
				and cc.cod_agrupador in ('102980','702018','910500','910650','910660','910670','803000','101900','910620')
				and not (em.cod_empresa = '1188' and cc.cod_agrupador = '702018') -- Solo para el Agr. 702018 en 1188 - Suma Allocation

			-- Para el Conjunto de CCs - Carga el Gasto de Operacion
			-- Cuenta Orig: 990337 - Cuenta Dest: 990350
			insert into #total_ln2 (origen, id_linea2, real, budget,
						id_mes, id_cc_orig, id_cc_opr, id_cliente, id_subclte, id_empresa,
						real_num, real_den, budget_num, budget_den, division, flag)
			select 'CALC' as origen, tgt.id_linea2, 
				st.real * -1 as real, 
				st.budget * -1 as budget,
				st.id_mes, st.id_cc_orig, st.id_cc_opr, st.id_cliente, st.id_subclte, st.id_empresa,
				st.real_num * -1 as real_num, 
				st.real_den * -1 as real_den, 
				st.budget_num * -1 as budget_num, 
				st.budget_den * -1 as budget_den, 
				st.division,
				1 -- Impide Duplicacion PK
			from #total_ln2 st, 
				(select * from dbo.DESG_LK_LINEA where cod_cuenta = '990337') src, 
				(select * from dbo.DESG_LK_LINEA where cod_cuenta = '990350') tgt,
				dbo.VW_LK_CC_OPR_HIST cc
			where st.id_linea2 = src.id_linea2
				and st.id_mes = cc.id_mes
				and st.id_cc_opr = cc.id_cc
				and cc.cod_agrupador in ('102980','702018','910500','910650','910660','910670','803000','101900','910620')

			set @msg = 'Excep. Allocation Branch: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg	

			-- Para Budget Quita cuentas q no se calculan
			-- 990372 Costo Capital
			update t
			set budget = 0, budget_num = 0, budget_den = 0
			from #total_ln2 t
			where (budget != 0 or budget_num != 0 or budget_den != 0)
				and exists (select 1 from DESG_LK_LINEA l
						where l.cod_cuenta in ('990372')
							and l.id_linea2 = t.id_linea2)

			set @msg = 'Reset Budget: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg	
		
		end

		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, '- Insert Ajuste en DW:'

		begin transaction
		
		delete ft
		from FT_OPR_DETALLE_CUENTA ft
		where ft.id_mes = @idMes
			and ft.origen = 'TOTAL'

		set @msg = 'Borrados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg		

		delete ft
		from dbo.FT_OPR_AGRUPADA ft
		where ft.id_mes = @idMes
			and ft.origen = 'TOTAL'

		set @msg = 'Borrados FT Agrup: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg		

				
		delete ft
		from FT_OPR_DETALLE ft
		where ft.id_mes = @idMes
			and ft.origen = 'TOTAL'

		set @msg = 'Borrados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		delete ft
		from FT_OPR ft
		where ft.id_mes = @idMes
			and ft.origen = 'TOTAL'

		set @msg = 'Borrados FT: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 
		
		-- Detalle Cuenta
		insert into dbo.FT_OPR_DETALLE_CUENTA (id_mes, origen, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget, real_num, real_den, budget_num, budget_den)
		select st.id_mes, 'TOTAL' as origen, st.id_cc_orig, 
			st.id_cc_opr, st.id_cliente, st.id_subclte, st.id_empresa, 
			lk.id_linea, 
			isnull(cc.id_cuenta,0) as id_cuenta,
			isnull(sr.id_tipo_serv_detalle,0) as id_tipo_serv_detalle,
			case when max(division) = 0 then 
				sum(real)
				else
					case when sum(isnull(real_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(real_num,0)) as float) / cast( sum(isnull(real_den,0)) as float) as numeric(22,4))
					end
			end as real,
			case when max(division) = 0 then 
				sum(budget)
				else
					case when sum(isnull(budget_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(budget_num,0)) as float) / cast( sum(isnull(budget_den,0)) as float) as numeric(22,4))
					end
			end as budget,
			sum(isnull(real_num,0)), 
			sum(isnull(real_den,0)),
			sum(isnull(budget_num,0)), 
			sum(isnull(budget_den,0))
		from #total_ln2 st,
			dbo.DESG_LK_LINEA lk
				left outer join dbo.DGEN_LK_CTA_CONTABLE cc
					on cc.cod_cuenta = lk.cod_cuenta
				left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
					on sr.cod_tipo_serv_detalle = lk.cod_servicio
		where lk.id_linea2 = st.id_linea2
			and st.origen = 'CALC'
		group by st.id_mes, st.id_cc_orig, 
			st.id_cc_opr, st.id_cliente, st.id_subclte, st.id_empresa, 
			lk.id_linea, isnull(cc.id_cuenta,0), isnull(sr.id_tipo_serv_detalle,0)
		having not ( sum(real) = 0 and sum(budget) = 0 and sum(isnull(real_num,0)) = 0 and sum(isnull(real_den,0)) = 0)

		set @msg = 'Insertados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Agrupada
		insert into dbo.FT_OPR_AGRUPADA (id_mes, 
			origen, id_cc_opr, id_agrupador, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget, real_num, real_den, budget_num, budget_den, division)
		select st.id_mes, 'TOTAL' as origen,
			st.id_cc_opr, cco.id_agrupador, st.id_empresa, 
			lk.id_linea, 
			isnull(cc.id_cuenta,0) as id_cuenta,
			isnull(sr.id_tipo_serv_detalle,0) as id_tipo_serv_detalle,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(real,0))
				else
					case when sum(isnull(real_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(real_num,0)) as float) / cast( sum(isnull(real_den,0)) as float) as numeric(22,4))
					end
			end as real,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(budget,0))
				else
					case when sum(isnull(budget_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(budget_num,0)) as float) / cast( sum(isnull(budget_den,0)) as float) as numeric(22,4))
					end
			end as budget,
			sum(isnull(real_num,0)) as real_num, 
			sum(isnull(real_den,0)) as real_den,
			sum(isnull(budget_num,0)) as budget_num, 
			sum(isnull(budget_den,0)) as budget_den,
			max(isnull(division,0)) as division
		from #total_ln2 st,
			dbo.VW_LK_CC_OPR_HIST cco,
			dbo.DESG_LK_LINEA lk
				left outer join dbo.DGEN_LK_CTA_CONTABLE cc
					on cc.cod_cuenta = lk.cod_cuenta
				left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
					on sr.cod_tipo_serv_detalle = lk.cod_servicio
		where lk.id_linea2 = st.id_linea2
			and st.id_cc_opr = cco.id_cc
			and st.id_mes = cco.id_mes
			and st.origen = 'CALC'
		group by st.id_mes, 
			st.id_cc_opr, cco.id_agrupador, st.id_empresa, 
			lk.id_linea, isnull(cc.id_cuenta,0), isnull(sr.id_tipo_serv_detalle,0)
		having not ( sum(real) = 0 and sum(budget) = 0 and sum(isnull(real_num,0)) = 0 and sum(isnull(real_den,0)) = 0)

		set @msg = 'Insertados FT Agrup: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Detalle
		insert into dbo.FT_OPR_DETALLE (id_mes, origen,
			id_cc_orig, id_cc_opr, id_cliente, 
			id_subclte, id_empresa, id_linea, 
			real, budget)
		select st.id_mes, 'TOTAL' as origen,
			st.id_cc_orig, st.id_cc_opr, st.id_cliente, 
			st.id_subclte, st.id_empresa, lk.id_linea,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(real,0))
				else
					case when sum(isnull(real_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(real_num,0)) as float) / cast( sum(isnull(real_den,0)) as float) as numeric(22,4))
					end
			end as real,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(budget,0))
				else
					case when sum(isnull(budget_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(budget_num,0)) as float) / cast( sum(isnull(budget_den,0)) as float) as numeric(22,4))
					end
			end as budget
		from #total_ln2 st,
			dbo.DESG_LK_LINEA lk
				left outer join dbo.DGEN_LK_CTA_CONTABLE cc
					on cc.cod_cuenta = lk.cod_cuenta
				left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
					on sr.cod_tipo_serv_detalle = lk.cod_servicio
		where lk.id_linea2 = st.id_linea2
			and st.origen = 'CALC'
		group by st.id_mes, st.id_cc_orig, 
			st.id_cc_opr, st.id_cliente, st.id_subclte, st.id_empresa, 
			lk.id_linea
		having not (sum(real) = 0 and sum(budget) = 0)

		set @msg = 'Insertados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		-- OPT
		insert into FT_OPR (id_mes, origen,
			id_cc_orig, id_cc_opr, id_linea, real, budget)
		select st.id_mes, 'TOTAL' as origen,
			st.id_cc_orig, st.id_cc_opr, lk.id_linea,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(real,0))
				else
					case when sum(isnull(real_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(real_num,0)) as float) / cast( sum(isnull(real_den,0)) as float) as numeric(22,4))
					end
			end as real,
			case when max(isnull(division,0)) = 0 then 
				sum(isnull(budget,0))
				else
					case when sum(isnull(budget_den,0)) = 0 then
						0
					else
						cast( cast(sum(isnull(budget_num,0)) as float) / cast( sum(isnull(budget_den,0)) as float) as numeric(22,4))
					end
			end as budget
		from #total_ln2 st,
			dbo.DESG_LK_LINEA lk
				left outer join dbo.DGEN_LK_CTA_CONTABLE cc
					on cc.cod_cuenta = lk.cod_cuenta
				left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
					on sr.cod_tipo_serv_detalle = lk.cod_servicio
		where lk.id_linea2 = st.id_linea2
			and st.origen = 'CALC'
		group by st.id_mes, st.id_cc_orig, 
			st.id_cc_opr, 
			lk.id_linea
		having not (sum(real) = 0 and sum(budget) = 0)

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
-- exec etl.map_ft_opr_totales 0, 202201