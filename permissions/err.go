package permissions

import (
	"fmt"
)

func (err *Error) Error() string {
	if err.Cause != nil {
		return fmt.Sprintf("%s: %s", err.Message, err.Cause.Error())
	}
	return err.Message
}
