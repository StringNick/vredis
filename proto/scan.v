module proto

const (
	err_wrong_result_type = error('wrong_result_type')
)

pub fn scan<T>(v Any) !T {
	$if T is string {
		scan_type_string(v)!
	}

	return err_wrong_result_type
}

pub fn scan_type_string(v Any) !string {
	match v {
		string {
			return string(v)
		}
		else {
			return proto.err_wrong_result_type
		}
	}
}

pub fn scan_type_int(v Any) !i64 {
	match v {
		i64 {
			return v
		}
		else {
			print('wrong_type $v')
			return proto.err_wrong_result_type
		}
	}
}

pub fn scan_type_string_slice(v Any) ![]string {
	match v {
		[]Any {
			mut result := []string{cap: v.len}

			for val in v {
				result << scan_type_string(val)!
			}

			return result
		}
		else {
			return proto.err_wrong_result_type
		}
	}
}
