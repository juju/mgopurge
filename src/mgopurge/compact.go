// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func compact(db *mgo.Database) error {
	engine, err := getStorageEngine(db)
	if err != nil {
		return fmt.Errorf("getting storage engine: %v", err)
	}
	switch engine {
	case "wiredTiger":
		logger.Infof("detected wiredTiger storage engine: compacting collections one by one")
		return compactAll(db)
	case "mmapv1":
		logger.Infof("detected mmapv1 storage engine: using repairDatabase to compact")
		return repairDatabase(db)
	default:
		return fmt.Errorf("unsupported storage engine: %v", engine)
	}
}

func getStorageEngine(db *mgo.Database) (string, error) {
	var result bson.M
	if err := db.Run(bson.D{{"serverStatus", 1}}, &result); err != nil {
		return "", err
	}
	rawSection, found := result["storageEngine"]
	if !found {
		// MongoDB 2.4 doesn't return a storageEngine section
		return "mmapv1", nil
	}
	section, ok := rawSection.(bson.M)
	if !ok {
		return "", errors.New("wrong type for storageEngine section")
	}
	rawName, found := section["name"]
	if !found {
		return "", errors.New("missing storageEngine name")
	}
	name, ok := rawName.(string)
	if !ok {
		return "", errors.New("storageEngine name is unexpected type")
	}
	return name, nil
}

// compactAll runs the "compact" command on all collections in the
// database. Under wiredTiger this returns disk space to the operating
// system. Each collection is locked while it is being compacted.
func compactAll(db *mgo.Database) error {
	names, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("obtaining collection names: %v", err)
	}
	for _, name := range names {
		if strings.HasPrefix(name, "system.") {
			continue
		}
		logger.Infof("compacting %s", name)
		err := db.Run(bson.D{
			{"compact", name},
			{"force", 1},
		}, nil)
		if err != nil {
			return fmt.Errorf("while compacting %s: %v", name, err)
		}
	}
	return nil
}

// repairDatabase runs the "repairDatabase" command. This rebuilds the
// database, locking it for the duration of the operation. Disk space
// used by removed objects will be returned to the operating
// system. This requires (db_size + 2GB) of free space.
func repairDatabase(db *mgo.Database) error {
	return db.Run(bson.M{"repairDatabase": 1}, nil)
}
