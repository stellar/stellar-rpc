package infrastructure

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

var testSalt = sha256.Sum256([]byte("a1"))

func getTestContract(name string) []byte {
	contractFile := path.Join(GetCurrentDirectory(), "../../../../../wasms/test_"+name+".wasm")
	ret, err := os.ReadFile(contractFile)
	if err != nil {
		str := fmt.Sprintf(
			"unable to read %s.wasm (%v) please run `make build-test-wasms` at the project root directory",
			name,
			err,
		)
		panic(str)
	}
	return ret
}

func GetHelloWorldContract() []byte {
	return getTestContract("hello_world")
}

func GetNoArgConstructorContract() []byte {
	return getTestContract("no_arg_constructor")
}

func GetEventsContract() []byte {
	return getTestContract("events")
}

func CreateInvokeHostOperation(
	sourceAccount string, contractID xdr.ContractId, method string, args ...xdr.ScVal,
) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol(method),
				Args:         args,
			},
		},
		Auth:          nil,
		SourceAccount: sourceAccount,
	}
}

func GetContractID(t testing.TB, sourceAccount string, salt [32]byte, networkPassphrase string) [32]byte {
	sourceAccountID := xdr.MustAddress(sourceAccount)
	preImage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: sha256.Sum256([]byte(networkPassphrase)),
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &xdr.ContractIdPreimageFromAddress{
					Address: xdr.ScAddress{
						Type:      xdr.ScAddressTypeScAddressTypeAccount,
						AccountId: &sourceAccountID,
					},
					Salt: salt,
				},
			},
		},
	}

	xdrPreImageBytes, err := preImage.MarshalBinary()
	require.NoError(t, err)
	hashedContractID := sha256.Sum256(xdrPreImageBytes)
	return hashedContractID
}

func CreateUploadHelloWorldOperation(sourceAccount string) *txnbuild.InvokeHostFunction {
	return CreateUploadWasmOperation(sourceAccount, GetHelloWorldContract())
}

func CreateUploadWasmOperation(sourceAccount string, contractCode []byte) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &contractCode,
		},
		SourceAccount: sourceAccount,
	}
}

func CreateCreateHelloWorldContractOperation(sourceAccount string) *txnbuild.InvokeHostFunction {
	contractHash := xdr.Hash(sha256.Sum256(GetHelloWorldContract()))
	salt := xdr.Uint256(testSalt)
	return createCreateContractOperation(sourceAccount, salt, contractHash)
}

func createCreateContractOperation(sourceAccount string, salt xdr.Uint256, contractHash xdr.Hash) *txnbuild.InvokeHostFunction {
	sourceAccountID := xdr.MustAddress(sourceAccount)
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
					FromAddress: &xdr.ContractIdPreimageFromAddress{
						Address: xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: &sourceAccountID,
						},
						Salt: salt,
					},
				},
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &contractHash,
				},
			},
		},
		Auth:          []xdr.SorobanAuthorizationEntry{},
		SourceAccount: sourceAccount,
	}
}

func CreateCreateNoArgConstructorContractOperation(sourceAccount string) *txnbuild.InvokeHostFunction {
	contractHash := xdr.Hash(sha256.Sum256(GetNoArgConstructorContract()))
	salt := xdr.Uint256(testSalt)
	return createCreateContractV2Operation(sourceAccount, salt, contractHash)
}

func CreateCreateEventsContractOperation(sourceAccount string) *txnbuild.InvokeHostFunction {
	contractHash := xdr.Hash(sha256.Sum256(GetEventsContract()))
	salt := xdr.Uint256(testSalt)
	return createCreateContractV2Operation(sourceAccount, salt, contractHash)
}

func CreateIncrementOperation(contractID xdr.ContractId, sourceAccount string) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol("increment"),
			},
		},
		Auth:          []xdr.SorobanAuthorizationEntry{},
		SourceAccount: sourceAccount,
	}
}

func createCreateContractV2Operation(
	sourceAccount string, salt xdr.Uint256, contractHash xdr.Hash,
) *txnbuild.InvokeHostFunction {
	sourceAccountID := xdr.MustAddress(sourceAccount)
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
			CreateContractV2: &xdr.CreateContractArgsV2{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
					FromAddress: &xdr.ContractIdPreimageFromAddress{
						Address: xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: &sourceAccountID,
						},
						Salt: salt,
					},
				},
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &contractHash,
				},
				ConstructorArgs: nil,
			},
		},
		Auth:          []xdr.SorobanAuthorizationEntry{},
		SourceAccount: sourceAccount,
	}
}
