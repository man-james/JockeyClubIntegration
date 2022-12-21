/****** Object:  Table [dbo].[occurrences]    Script Date: 21/12/2022 10:42:12 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[occurrences](
	[occurrenceId] [nvarchar](50) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[json] [nvarchar](max) NOT NULL,
	[error] [varchar](128) NULL,
	[send] [bit] NOT NULL,
 CONSTRAINT [PK_occurrences] PRIMARY KEY CLUSTERED 
(
	[occurrenceId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[occurrences] ADD  DEFAULT ((0)) FOR [send]
GO


