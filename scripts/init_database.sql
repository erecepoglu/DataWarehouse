-- ==================================================================================
-- Script Name:   Create_DataWarehouse_Medallion.sql
-- Author:        enes
-- Create Date:   2026-02-27
-- Description:   Initializes the 'DataWarehouse' database and sets up the 
--                foundational schemas (bronze, silver, gold) for a Medallion 
--                Architecture data model. 
--                
--                *** WARNING: DESTRUCTIVE SCRIPT ***
--                This script will forcibly drop the 'DataWarehouse' database 
--                and all its existing data if it already exists.
--
-- Database:      master (Initialization) -> DataWarehouse (Schema Creation)
-- Schema:        dbo (Default), bronze, silver, gold
-- Dependencies:  Requires sysadmin or dbcreator server-level roles to execute.
--
-- Execution:     Run the entire script at once via SSMS, Azure Data Studio, 
--                or sqlcmd. Ensure no critical processes are connected to 
--                'DataWarehouse' before executing.
-- ==================================================================================
-- Modification History:
-- Date         Author          Description
-- ----------   -------------   -----------------------------------------------------
-- 2026-02-27   enes     Initial creation. Database and Medallion schemas.
-- ==================================================================================

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating Schemas of Medallion Architechure
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
