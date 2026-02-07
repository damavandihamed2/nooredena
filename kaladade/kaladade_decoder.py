import json
import base64
import datetime
import urllib.parse


class KaladadeDecoder:
    """
    Class-based decoder for the site's encoded JSON responses.

    Usage:
        decoder = KaladadeDecoder(kdc1=session_id, kdc2=lgnd)
        decoded_response = decoder.decode(response_json_dict)
    """

    def __init__(self, kdc1: str, kdc2: str):
        self.kdc1 = kdc1
        self.kdc2 = kdc2

    # ------------------------
    # low-level helpers
    # ------------------------
    @staticmethod
    def _d_get_code(s: str, idx: int) -> int:
        return ord(s[idx])

    @staticmethod
    def _d_get_chr(code: int) -> str:
        return chr(code)

    # fna0, fne4, fnf5, fng6, fnh7, fni8, fnj9 helpers
    @staticmethod
    def _fna0_m(ne: int, se: int, de: int) -> int:
        return ne % 10 + (2 * se) ** 2 + de

    @staticmethod
    def _fna0_x(seq: str, idx: int, cnt: int, sign: int) -> int:
        # d_get_code(seq, idx) + cnt * sign
        return KaladadeDecoder._d_get_code(seq, idx) + cnt * sign

    @staticmethod
    def _fne4_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % (4 * mt) + sign * mt

    @staticmethod
    def _fne4_x(seq: str, idx: int, Fe: int) -> int:
        return KaladadeDecoder._d_get_code(seq, idx) - Fe

    @staticmethod
    def _fnf5_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % 14 - sign * mt

    @staticmethod
    def _fng6_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % 17 - sign * mt

    @staticmethod
    def _fnh7_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % 16 + sign * mt + mt

    @staticmethod
    def _fni8_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % 24 + sign * mt

    @staticmethod
    def _fnj9_t(idx: int, sign: int, r: int, key: list[int], mt: int) -> int:
        return key[idx] % 18 + sign * mt

    # ------------------------
    # _dx / _dy / base64
    # ------------------------
    def _dx(self) -> int:
        day = datetime.datetime.now(datetime.UTC).day
        ne = self._d_get_code(self.kdc1, day)
        se = 1
        if self.kdc2 and len(self.kdc2) > 20:
            se = self._d_get_code(self.kdc2, day)
        return ne % 7 + se % 11

    def _dy(self) -> list[int]:
        key = self.kdc1
        if self.kdc2 and len(self.kdc2) > 20:
            key += self.kdc2
        return [ord(c) for c in key]

    @staticmethod
    def _base64_decode(s: str) -> str:
        return urllib.parse.unquote(base64.b64decode(s).decode("utf-8"))

    # ------------------------
    # decoder functions a0..j9
    # ------------------------
    def _a0(self, ie: dict, M: int, R: int, _z) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = self._fna0_m(M, sign, R)
        for i in range(len(ne)):
            cnt += 1
            if cnt > Xe:
                cnt = 1
            code = self._fna0_x(ne, i, cnt, sign)
            ne[i] = self._d_get_chr(code)
            sign *= -1
        return ne

    def _b1(self, ie: dict, K: int, pe: int, _z) -> list[str] | None:
        Me = list(ie["Data"])
        if len(Me) < 3:
            return None
        ye = -1
        je = 0
        Kt = (K % 10) + pe
        for idx in range(len(Me)):
            je += 1
            if je > Kt:
                je = 1
            code = self._d_get_code(Me, idx) + je * ye
            Me[idx] = self._d_get_chr(code)
            ye *= -1
        return Me

    def _c2(self, ie: dict, M: int, R: int, _z) -> list[str] | None:
        se = list(ie["Data"])
        if len(se) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = 2 * R
        for idx in range(len(se)):
            cnt += 1
            if cnt > Xe:
                cnt = 1
            code = self._d_get_code(se, idx) + cnt * sign
            se[idx] = self._d_get_chr(code)
            sign *= -1
        return se

    def _d3(self, ie: dict, x: int, Y: int, _z) -> list[str] | None:
        Re = list(ie["Data"])
        if len(Re) < 3:
            return None
        pt = -1
        ie_cnt = 0
        ke = Y // 2
        He = (x % 10) + Y
        for idx in range(len(Re)):
            ie_cnt += 1
            if ie_cnt > He:
                ie_cnt = 1
            code = self._d_get_code(Re, idx) + ie_cnt * pt + ke
            Re[idx] = self._d_get_chr(code)
            pt *= -1
        return Re

    def _e4(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z)
        mt = M % 3 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fne4_t(cnt, sign, R, z, mt) + R
            code = self._fne4_x(ne, i, Fe)
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    def _f5(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z)
        mt = M % 4 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fnf5_t(cnt, sign, R, z, mt) + R
            code = self._d_get_code(ne, i) - Fe
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    def _g6(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z)
        mt = M % 5 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fng6_t(cnt, sign, R, z, mt) + R
            code = self._d_get_code(ne, i) - Fe
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    def _h7(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z)
        mt = M % 6 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fnh7_t(cnt, sign, R, z, mt) + R
            code = self._d_get_code(ne, i) - Fe
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    def _i8(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z) - R
        mt = M % 7 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fni8_t(cnt, sign, R, z, mt) + R
            code = self._d_get_code(ne, i) - Fe
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    def _j9(self, ie: dict, M: int, R: int, z: list[int]) -> list[str] | None:
        ne = list(ie["Data"])
        if len(ne) < 3:
            return None
        sign = -1
        cnt = 0
        Xe = len(z)
        mt = M % 8 + 1
        for i in range(len(ne)):
            if cnt >= Xe:
                cnt = 0
            Fe = self._fnj9_t(cnt, sign, R, z, mt) + R
            code = self._d_get_code(ne, i) - Fe
            ne[i] = self._d_get_chr(code)
            cnt += 1
            sign *= -1
        return ne

    # ------------------------
    # public API
    # ------------------------
    def decode(self, response: dict) -> dict:
        """
        response: dict شبیه:
        {
            "ResultCode": 228,
            "Data": "<encoded string>",
            "Message": "...",
            "ToastType": ...
        }
        """
        P = response.get("ResultCode", 0)
        K = P % 10

        dx_val = self._dx()
        dy_val = self._dy() if K >= 4 else None

        functions = {
            0: self._a0,
            1: self._b1,
            2: self._c2,
            3: self._d3,
            4: self._e4,
            5: self._f5,
            6: self._g6,
            7: self._h7,
            8: self._i8,
            9: self._j9,
        }

        func = functions.get(K)
        if not func:
            return response

        chars = func(response, P, dx_val, dy_val)
        if not chars:
            return response

        decoded_text = self._base64_decode("".join(chars))
        response["Data"] = json.loads(decoded_text)

        return response

