CREATE TABLE TestDB.dbo.Events (
	Id int IDENTITY(1,1) NOT NULL,
	Name varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	EventTime datetime NOT NULL,
	CONSTRAINT PK_Events PRIMARY KEY (Id)
);

CREATE TABLE TestDB.dbo.Test (
	Id int IDENTITY(1,1) NOT NULL,
	Value varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	CONSTRAINT PK_Test PRIMARY KEY (Id)
);

CREATE PROCEDURE dbo.TestProcedure
  @InputParameter INT,
  @MaxEvent INT OUTPUT,
  @ProcName VARCHAR(100) OUTPUT
AS 
BEGIN
  SELECT @MaxEvent = (SELECT MAX(Id) FROM dbo.Events);
  SELECT @ProcName = (SELECT OBJECT_NAME(@@PROCID));
  SELECT 'TestProcedure was executed';
 
  RETURN @InputParameter;
END;
