using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace JobSvc.Migrations
{
    /// <inheritdoc />
    public partial class FixStatusLowercaseAndDefaults : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<string>(
                name: "Status",
                table: "Jobs",
                type: "text",
                nullable: false,
                defaultValueSql: "'queued'",
                oldClrType: typeof(string),
                oldType: "text");

            migrationBuilder.AlterColumn<int>(
                name: "RetryCount",
                table: "Jobs",
                type: "integer",
                nullable: false,
                defaultValueSql: "0",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "Printed",
                table: "Jobs",
                type: "integer",
                nullable: false,
                defaultValueSql: "0",
                oldClrType: typeof(int),
                oldType: "integer");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<string>(
                name: "Status",
                table: "Jobs",
                type: "text",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "text",
                oldDefaultValueSql: "'queued'");

            migrationBuilder.AlterColumn<int>(
                name: "RetryCount",
                table: "Jobs",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldDefaultValueSql: "0");

            migrationBuilder.AlterColumn<int>(
                name: "Printed",
                table: "Jobs",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldDefaultValueSql: "0");
        }
    }
}
