package config

import (
	"fmt"
	"reflect"
	"time"

	"github.com/spf13/pflag"
)

// Every TOML leaf of Config is settable from the command line as a flag named
// by its dotted TOML path:
//
//	--storage.default_data_dir=/var/stellar
//	--service.methods.getLedgers.queue_limit=500
//	--service.methods.queue_limit=30          (the methods-wide default tier)
//
// The flag set is DERIVED from the Config struct by reflection over the `toml`
// tags — there is no second declaration list that could drift out of lockstep
// with the file schema (v1's failure mode). Adding a field to any config struct
// automatically creates its flag; TestBindFlags_LockstepWithTOMLSchema pins the
// correspondence.
//
// Precedence is implemented by ORDER, not by these functions: the daemon
// decodes the file, then ApplyFlags overlays only the flags the user actually
// set, then WithDefaults resolves the methods cascade and fills defaults. A
// flag therefore beats the file at the SAME specificity tier, while a more
// specific file value still beats a less specific flag (specificity beats
// source).

// FlagOverrides is the surface ApplyFlags needs from *pflag.FlagSet; an
// interface so config doesn't force a pflag dependency on every caller of
// LoadConfigWithFlags (nil = no overrides).
type FlagOverrides interface {
	Changed(name string) bool
	GetString(name string) (string, error)
	GetUint(name string) (uint, error)
	GetUint32(name string) (uint32, error)
	GetInt(name string) (int, error)
	GetDuration(name string) (time.Duration, error)
	GetStringSlice(name string) ([]string, error)
	GetStringToString(name string) (map[string]string, error)
}

var _ FlagOverrides = (*pflag.FlagSet)(nil)

// BindFlags registers one override flag per TOML leaf of Config on fs. Call it
// once on the root command's flag set, before parsing.
func BindFlags(fs *pflag.FlagSet) {
	walkLeaves(reflect.ValueOf(&Config{}).Elem(), "", func(path string, f reflect.Value) {
		usage := "overrides " + path + " from the config file"
		switch leafKind(f.Type()) {
		case kindString:
			fs.String(path, "", usage)
		case kindUint:
			fs.Uint(path, 0, usage)
		case kindUint32:
			fs.Uint32(path, 0, usage)
		case kindInt:
			fs.Int(path, 0, usage)
		case kindDuration:
			fs.Duration(path, 0, usage)
		case kindStringSlice:
			fs.StringSlice(path, nil, usage)
		case kindStringMap:
			fs.StringToString(path, nil, usage)
		case kindUnsupported:
			// unreachable: walkLeaves panics on an unsupported leaf before visiting
		}
	})
}

// ApplyFlags writes every flag the user actually set (fs.Changed) into cfg,
// allocating pointer fields as needed. Run it AFTER DecodeConfig and BEFORE
// WithDefaults so the overlay participates in defaulting at its own tier.
func ApplyFlags(cfg *Config, fs FlagOverrides) error {
	var firstErr error
	walkLeaves(reflect.ValueOf(cfg).Elem(), "", func(path string, f reflect.Value) {
		if firstErr != nil || !fs.Changed(path) {
			return
		}
		if err := setLeaf(f, path, fs); err != nil {
			firstErr = err
		}
	})
	return firstErr
}

//nolint:cyclop // one case per supported leaf type; splitting hides the correspondence
func setLeaf(f reflect.Value, path string, fs FlagOverrides) error {
	fail := func(err error) error { return fmt.Errorf("apply flag --%s: %w", path, err) }
	switch leafKind(f.Type()) {
	case kindString:
		v, err := fs.GetString(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindUint:
		v, err := fs.GetUint(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindUint32:
		v, err := fs.GetUint32(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindInt:
		v, err := fs.GetInt(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindDuration:
		v, err := fs.GetDuration(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindStringSlice:
		v, err := fs.GetStringSlice(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindStringMap:
		v, err := fs.GetStringToString(path)
		if err != nil {
			return fail(err)
		}
		setPossiblyPointer(f, reflect.ValueOf(v))
	case kindUnsupported:
		// unreachable: walkLeaves panics on an unsupported leaf before visiting
	}
	return nil
}

// setPossiblyPointer assigns v to f, allocating first when f is a pointer field
// (the pointer-typed optionals: a set flag means "explicitly set", so it always
// lands as a non-nil pointer).
func setPossiblyPointer(f, v reflect.Value) {
	if f.Kind() == reflect.Pointer {
		p := reflect.New(f.Type().Elem())
		p.Elem().Set(v.Convert(f.Type().Elem()))
		f.Set(p)
		return
	}
	f.Set(v.Convert(f.Type()))
}

type kind int

const (
	kindUnsupported kind = iota
	kindString
	kindUint
	kindUint32
	kindInt
	kindDuration
	kindStringSlice
	kindStringMap
)

func durationType() reflect.Type { return reflect.TypeFor[time.Duration]() }

// leafKind classifies a (possibly pointer-typed) leaf field. time.Duration must
// be checked by TYPE before any Kind switch — its Kind is int64.
//
//nolint:exhaustive // every unlisted reflect.Kind falls through to unsupported
func leafKind(t reflect.Type) kind {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == durationType() {
		return kindDuration
	}
	switch t.Kind() {
	case reflect.String:
		return kindString
	case reflect.Uint:
		return kindUint
	case reflect.Uint32:
		return kindUint32
	case reflect.Int:
		return kindInt
	case reflect.Slice:
		if t.Elem().Kind() == reflect.String {
			return kindStringSlice
		}
	case reflect.Map:
		if t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.String {
			return kindStringMap
		}
	}
	return kindUnsupported
}

// walkLeaves visits every flag-eligible leaf of a config struct value, calling
// visit with the leaf's dotted TOML path and its field Value. Rules:
//
//   - every exported field must carry a `toml` tag ("-" to opt out); an
//     untagged exported field PANICS at BindFlags time. go-toml matches
//     untagged exported fields by NAME, so a forgotten tag would silently
//     become a settable file key with no flag — the one way the file schema
//     and the flag set could drift apart. (Config deliberately contains no
//     SDK struct types for the same reason — it mirrors them as BSBConfig and
//     DataStoreConfig, whose untagged SDK fields like NetworkPassphrase must
//     not be file keys.)
//   - a tagged struct field recurses with its tag as a path segment;
//   - a tagged leaf of an unsupported type PANICS at BindFlags time — adding a
//     config field of a new type must extend leafKind, not silently lose its flag.
func walkLeaves(v reflect.Value, prefix string, visit func(path string, f reflect.Value)) {
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("toml")
		if tag == "" {
			panic(fmt.Sprintf("config flags: exported field %s.%s has no toml tag — go-toml would "+
				"still accept it as a file key (matched by field name) but it would get no flag; "+
				"tag it, or mark it `toml:\"-\"` to exclude it from the file schema", t.Name(), field.Name))
		}
		if tag == "-" {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + "." + tag
		}
		f := v.Field(i)
		ft := field.Type
		if ft.Kind() == reflect.Struct && ft != durationType() {
			walkLeaves(f, path, visit)
			continue
		}
		if leafKind(ft) == kindUnsupported {
			panic(fmt.Sprintf("config flags: field %s (%s) has no flag mapping; extend leafKind", path, ft))
		}
		visit(path, f)
	}
}
