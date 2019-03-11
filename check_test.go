package ok

/*
func TestVersion(t *testing.T) {
	if assert.True(t, (&Version{1, 2, 3}).Equal(&Version{1, 2, 3})) {

	}
	if assert.True(t, (&Version{1, 2, 3}).Less(&Version{1, 2, 4})) {
		assert.True(t, (&Version{1, 2, 3}).Less(&Version{2, 0, 0}))
		assert.True(t, (&Version{2, 0, 3}).Less(&Version{2, 1, 0}))
	}
	if assert.False(t, (&Version{2, 2, 3}).Less(&Version{1, 2, 4})) {
		assert.False(t, (&Version{2, 2, 3}).Less(&Version{2, 0, 0}))
		assert.False(t, (&Version{2, 1, 3}).Less(&Version{2, 1, 0}))
	}
	clickhouse, err := Connect("tcp://127.0.0.1:9000?debug=0")
	if err != nil {
		t.Fatal(err)
	}
	if version, err := clickhouse.Version(); assert.NoError(t, err) {
		switch {
		case version.Less(&Version{18, 0, 0}):
			t.Logf("old version: %s", version)
		case version.Less(&Version{19, 0, 0}):
			t.Logf("version 18 X: %s", version)
		case version.Less(&Version{20, 0, 0}):
			t.Logf("version 19 X: %s", version)
		default:
			t.Logf("version: %s", version)
		}
	}
}

func TestBase(t *testing.T) {
	clickhouse, err := Connect("tcp://127.0.0.1:9000?debug=0")
	if err != nil {
		t.Fatal(err)
	}

	if databases, err := clickhouse.ShowDatabases(); assert.NoError(t, err) {
		if assert.True(t, len(databases) != 0) {
			var exists bool
			for _, database := range databases {
				if database == "system" {
					exists = true
					break
				}
			}
			assert.True(t, exists)
		}
	}

	if tables, err := clickhouse.ShowTables("system"); assert.NoError(t, err) {
		if assert.True(t, len(tables) != 0) {
			var exists bool
			for _, table := range tables {
				if table == "settings" {
					exists = true
					break
				}
			}
			assert.True(t, exists)
		}
	}

	if exists, err := clickhouse.DatabaseExists("system"); assert.NoError(t, err) {
		if assert.True(t, exists) {
			if exists, err = clickhouse.DatabaseExists("not-exists"); assert.NoError(t, err) {
				assert.False(t, exists)
			}
		}
	}

	if exists, err := clickhouse.TableExists("system", "settings"); assert.NoError(t, err) {
		if assert.True(t, exists) {
			if exists, err = clickhouse.TableExists("system", "not-exists"); assert.NoError(t, err) {
				assert.False(t, exists)
			}
		}
	}
}

func TestDictionary(t *testing.T) {
	clickhouse, err := Connect("tcp://127.0.0.1:9000?debug=0")
	if err != nil {
		t.Fatal(err)
	}
	if err := clickhouse.ReloadDictionary("not-exists"); assert.Error(t, err) {
		if exception, ok := err.(*ch.Exception); assert.True(t, ok) {
			assert.Equal(t, int32(36), exception.Code)
		}
	}
	if exists, err := clickhouse.DictionaryExists("dictionary"); assert.NoError(t, err) {
		if exists {
			assert.NoError(t, clickhouse.ReloadDictionary("dictionary"))
		}
	}
}
*/
