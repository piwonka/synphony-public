package helpers

import (
	"fmt"
	"strconv"

	"github.com/db47h/rand64/splitmix64"
)

// Due to pythons inconsistent integer behavior for different math libraries ( c-types vs. int vs. long int), this mask is used to normalize values to the proper format before using it with such a library
const signed32Mask = uint32(0x7FFFFFFF)

/* Normalize 64 bit uint to 32 bit uint using a mask */
func normalizeSeedToUint32(seed uint64) uint32 {
	return uint32(seed) & signed32Mask
}

/* Parse a 32 bit uint from a string, returns an error if the string does not contain a valid 32 bit uint*/
func parseSeedString(seedStr string) (uint32, error) {
	if seedStr == "" {
		return 0, fmt.Errorf("empty seed string")
	}
	seed, err := strconv.ParseUint(seedStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid seed %q: %w", seedStr, err)
	}
	return uint32(seed), nil
}

/* Generate a seed using the current seed state. Uses the suppied master seed for generaiton if an empty lastSeed is provided
*  Return an error if a necessary seed is not properly provided.
 */
func GenerateWorkerSeed(lastSeed string, masterSeed string) (string, error) {
	var base uint64
	if lastSeed == "" {
		master, err := parseSeedString(masterSeed)
		if err != nil {
			return "", fmt.Errorf("invalid Master Seed: %w", err)
		}
		base = uint64(master)
	} else {
		last, err := parseSeedString(lastSeed)
		if err != nil {
			return "", fmt.Errorf("invalid last worker seed: %w", err)
		}
		base = uint64(last)
	}

	src := splitmix64.New(
		base,
	) // seed randum number generator using master Seed or last known worker GenerateWorkerSeed
	next := normalizeSeedToUint32(src.Uint64())      // generate a seed and normalize it to uint32
	return strconv.FormatUint(uint64(next), 10), nil //return the generated seed as a string
}

/* Apply the stage seed to an existing Worker Seed using XOR */
func GenerateWorkerSeedForStage(workerSeedStr string, stageSeedStr string) (string, error) {
	jobSeed, err := parseSeedString(workerSeedStr)
	if err != nil {
		return "", fmt.Errorf("invalid job seed: %w", err)
	}
	stageSeed, err := strconv.ParseUint(stageSeedStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid stage master seed %q:%w", stageSeed, err)
	}
	mixed := uint64(jobSeed) ^ stageSeed // XOR the seeds
	src := splitmix64.New(
		mixed,
	) // seed splitmix prng with the mixed seed
	newWorkerSeed := normalizeSeedToUint32(src.Uint64())      // normalize the seed
	return strconv.FormatUint(uint64(newWorkerSeed), 10), nil // return the seed as a string
}
