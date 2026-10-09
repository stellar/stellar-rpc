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
	GetBool(name string) (bool, error)
	GetUint(name string) (uint, error)
	GetUint32(name string) (uint32, error)
	GetInt(name string) (int, error)
	GetInt64(name string) (int64, error)
	GetDuration(name string) (time.Duration, error)
	GetStringSlice(name string) ([]string, error)
	GetStringToString(name string) (map[string]string, error)
}

// BindFlags registers one override flag per TOML leaf of Config on fs. Call it
// once on the root command's flag set, before parsing.
func BindFlags(fs *pflag.FlagSet) {
	walkLeaves(reflect.ValueOf(&Config{}).Elem(), "", func(path string, f reflect.Value) {
		usage := "overrides " + path + " from the config file"
		switch leafKind(f.Type()) {
		case kindString:
			fs.String(path, "", usage)
		case kindBool:
			// The false default is irrelevant — ApplyFlags reads a flag only when
			// the user set it. Turning a true-by-default key off needs --key=false,
			// since a bare --key sets true.
			fs.Bool(path, false, usage)
		case kindUint:
			fs.Uint(path, 0, usage)
		case kindUint32:
			fs.Uint32(path, 0, usage)
		case kindInt:
			fs.Int(path, 0, usage)
		case kindInt64:
			fs.Int64(path, 0, usage)
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
// allocating pointer fields as needed. Map-valued flags merge into the file's
// map key by key (a flag can set an entry, never delete one); slice-valued
// flags replace the file's slice wholesale (a list has no per-element identity
// to merge on). Run it AFTER DecodeConfig and BEFORE WithDefaults so the
// overlay participates in defaulting at its own tier.
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

func setLeaf(f reflect.Value, path string, fs FlagOverrides) error {
	// Every scalar kind does the same three things and differs only in which
	// getter it calls, so each case is that one call and the shared get-check-
	// assign lives once, below.
	var (
		v   any
		err error
	)
	switch leafKind(f.Type()) {
	case kindString:
		v, err = fs.GetString(path)
	case kindBool:
		v, err = fs.GetBool(path)
	case kindUint:
		v, err = fs.GetUint(path)
	case kindUint32:
		v, err = fs.GetUint32(path)
	case kindInt:
		v, err = fs.GetInt(path)
	case kindInt64:
		v, err = fs.GetInt64(path)
	case kindDuration:
		v, err = fs.GetDuration(path)
	case kindStringSlice:
		v, err = fs.GetStringSlice(path)
	case kindStringMap:
		// The one kind that is not a plain assignment.
		return setMapLeaf(f, path, fs)
	case kindUnsupported:
		// unreachable: walkLeaves panics on an unsupported leaf before visiting
		return nil
	}
	if err != nil {
		return fmt.Errorf("apply flag --%s: %w", path, err)
	}
	setPossiblyPointer(f, reflect.ValueOf(v))
	return nil
}

// setMapLeaf merges per key rather than replacing the map: a map flag names
// individual entries, and clobbering the file's sibling entries is never what
// the operator meant.
func setMapLeaf(f reflect.Value, path string, fs FlagOverrides) error {
	v, err := fs.GetStringToString(path)
	if err != nil {
		return fmt.Errorf("apply flag --%s: %w", path, err)
	}
	if f.IsNil() {
		f.Set(reflect.MakeMap(f.Type()))
	}
	for k, val := range v {
		f.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(val))
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
	kindBool
	kindUint
	kindUint32
	kindInt
	kindInt64
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
	case reflect.Bool:
		return kindBool
	case reflect.Uint:
		return kindUint
	case reflect.Uint32:
		return kindUint32
	case reflect.Int:
		return kindInt
	case reflect.Int64:
		// time.Duration is also Int64-kinded and is matched above, before this.
		return kindInt64
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
//     and the flag set could drift apart (see DataStoreConfig for the same
//     concern with embedded SDK structs);
//   - an untagged anonymous struct field recurses with the SAME
//     prefix, matching the decoder's flattening of embedded structs;
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
		if tag == "" && field.Anonymous && field.Type.Kind() == reflect.Struct {
			walkLeaves(v.Field(i), prefix, visit)
			continue
		}
		if tag == "" {
			panic(fmt.Sprintf("config flags: exported field %s.%s has no toml tag — go-toml would "+
				"accept it as a file key by field name, but it would get no flag; "+
				"tag it, or mark it toml:\"-\"", t.Name(), field.Name))
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
