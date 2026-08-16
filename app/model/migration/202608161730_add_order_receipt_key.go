package migration

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

func m202608161730AddOrderReceiptKey() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202608161730_add_order_receipt_key",
		Migrate: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_order") {
				return nil
			}
			return tx.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_bep_order_receipt_key_unique ON bep_order (ref_receipt_key) WHERE ref_receipt_key <> ''").Error
		},
		Rollback: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_order") {
				return nil
			}
			return tx.Exec("DROP INDEX IF EXISTS idx_bep_order_receipt_key_unique").Error
		},
	}
}
