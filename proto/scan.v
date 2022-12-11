module proto

const (
	err_wrong_result_type = error('wrong_result_type')
)

pub fn scan[T](v Any) !T {
	$if T is string {
		return scan_type_string(v)
	} $else $if T is i64 {
		return scan_type_int(v)
	} $else $if T is []string {
		return scan_type_string_slice(v)
	} $else $if T is map[string]string {
		return scan_type_string_map(v)
	} $else $if T is map[string]Any {
		return scan_type_map(v)
	} $else {
		println('unknown scan type')
	}

	return proto.err_wrong_result_type
}

fn scan_type_string(v Any) !string {
	match v {
		string {
			return string(v)
		}
		else {
			return proto.err_wrong_result_type
		}
	}
}

fn scan_type_int(v Any) !i64 {
	match v {
		i64 {
			return v
		}
		else {
			print('wrong_type ${v}')
			return proto.err_wrong_result_type
		}
	}
}

fn scan_type_string_slice(v Any) ![]string {
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

fn scan_type_map(v Any) !map[string]Any {
	match v {
		map[string]Any {
			return v
		}
		else {
			return proto.err_wrong_result_type
		}
	}
}

fn scan_type_string_map(v Any) !map[string]string {
	match v {
		map[string]Any {
			mut result := map[string]string{}

			for k, val in v {
				result[k] = scan_type_string(val)!
			}

			return result
		}
		else {
			return proto.err_wrong_result_type
		}
	}
}
