package migration

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

// Signed payment-resume URLs can exceed the legacy 255-character limit.
func m202607261930WidenOrderReturnURL() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202607261930_widen_order_return_url",
		Migrate: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_order") {
				return nil
			}
			if tx.Dialector.Name() != "postgres" {
				return nil
			}
			return tx.Exec("ALTER TABLE bep_order ALTER COLUMN return_url TYPE TEXT").Error
		},
		Rollback: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_order") || tx.Dialector.Name() != "postgres" {
				return nil
			}
			return tx.Exec("ALTER TABLE bep_order ALTER COLUMN return_url TYPE VARCHAR(255)").Error
		},
	}
}
