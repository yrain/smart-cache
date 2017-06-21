new Vue({
    el: '#app',
    data() {
        return {
            filter: '',
            keyFilter: '',
            hostFilter: '',
            namesLoading: false,
            keysLoading: false,
            redisLoading: false,
            hostsLoading: false,
            names: [],
            keys: [],
            hosts: [],
            name: '',
            key: '',
            host: ''
        }
    },
    mounted() {
        const vm = this;
        vm.reload();
    },
    computed: {
        filterNames() {
            const vm = this, filter = vm.filter, names = vm.names;
            return names.filter(name => name.toUpperCase().indexOf(filter.toUpperCase()) !== -1);
        },
        filterKeys() {
            const vm = this, filter = vm.keyFilter, keys = vm.keys;
            return keys.filter(key => key.key.toUpperCase().indexOf(filter.toUpperCase()) !== -1);
        },
        filterHosts() {
            const vm = this, filter = vm.hostFilter, hosts = vm.hosts;
            return hosts.filter(host => host.id.toUpperCase().indexOf(filter.toUpperCase()) !== -1);
        }
    },
    watch: {
        name() {
            const vm = this;
            vm.nameChanged();
        },
        key() {
            const vm = this;
            vm.keyChanged();
        },
        host() {
            const vm = this;
            vm.hostChanged();
        }
    },
    methods: {
        resetAll() {
            const vm = this;
            vm.resetKeys();
            vm.name = '';
        },
        resetKeys() {
            const vm = this;
            vm.resetHosts();
            vm.keyFilter = '';
            vm.key = '';
        },
        resetHosts() {
            const vm = this;
            vm.hostFilter = '';
            vm.host = null;
        },
        clearFilter() {
            this.filter = '';
        },
        clearKeyFilter() {
            this.keyFilter = '';
        },
        clearHostFilter() {
            this.hostFilter = '';
        },
        selectName(name) {
            this.name = name;
        },
        selectKey(selection) {
            this.key = selection.key;
        },
        selectHost(selection) {
            this.host = selection;
        },
        nameChanged() {
            const vm = this;
            vm.reloadKeys();
        },
        keyChanged() {
            const vm = this;
            vm.reloadHosts();
            const $redis = document.querySelector('#redis .content-wrapper');
            $redis.innerHTML = '';
            if (!vm.name || !vm.key) {
                return;
            }
            vm.$get(vm.name, vm.key).then(result => {
                const formatter = new JSONFormatter(result);
                $redis.appendChild(formatter.render());
                formatter.openAtDepth(3);
            });
        },
        hostChanged() {
            const vm = this;
            const $host = document.querySelector('#host .content-wrapper');
            $host.innerHTML = '';
            if (vm.host) {
                const formatter = new JSONFormatter(vm.host);
                $host.appendChild(formatter.render());
                formatter.openAtDepth();
            }
        },
        rem(name, $event) {
            const vm = this;
            $event.stopPropagation();
            vm._confirm('Remove It?', 'Confirm', function (confirm) {
                if (!confirm) {
                    return;
                }
                vm.$rem(name).then(success => {
                    if (!success) {
                        vm.$message.error('error.');
                        return;
                    }
                    vm.$message.info('success.');
                    vm.reload();
                });
            });
        },
        reload() {
            const vm = this;
            vm.resetAll();
            vm.reloadKeys();
            vm.$names().then(data => {
                vm.names = data;
            });
        },
        reloadKeys() {
            const vm = this;
            vm.resetKeys();
            if (!vm.name) {
                vm.keys = [];
                return;
            }
            vm.$keys(vm.name).then(data => {
                vm.keys = data;
            });
        },
        reloadHosts() {
            const vm = this;
            vm.resetHosts();
            if (!vm.name || !vm.key) {
                vm.hosts = [];
                return;
            }
            vm.$fetch(vm.name, vm.key).then(data => {
                vm.hosts = data;
            });
        },
        del(key, $event) {
            const vm = this;
            $event.stopPropagation();
            vm._confirm('Remove it?', 'Confirm', function (confirm) {
                if (!confirm) {
                    return;
                }
                vm.$del(vm.name, key).then(success => {
                    if (!success) {
                        vm.$message.error('error.');
                        return;
                    }
                    vm.$message.info('success.');
                    vm.reloadKeys();
                });
            });
        },
        cls() {
            const vm = this;
            vm._confirm('Clean all?', 'Confirm', function (confirm) {
                if (!confirm) {
                    return;
                }
                vm.$cls().then(success => {
                    if (!success) {
                        vm.$message.warning('Fail');
                        return;
                    }
                    vm.$message.info('Success');
                    vm.reload();
                });
            })
        },
        $names() {
            let vm = this;
            return new Promise((resolve) => {
                vm.namesLoading = true;
                vm._get(`names.json`).then((result) => {
                    vm.namesLoading = false;
                    if (result.code === -1) {
                        vm.$message.info(result.msg || 'Fail');
                        resolve([]);
                        return;
                    }
                    let data = result.data || [];
                    data.sort((a, b) => {
                        return a.localeCompare(b);
                    });
                    resolve(data);
                }).catch(() => {
                    vm.namesLoading = false;
                    vm.$message.error('Error!');
                    resolve([]);
                });
            });
        },
        $keys(name) {
            let vm = this;
            return new Promise((resolve) => {
                vm.keysLoading = true;
                vm._get(`keys.json?name=${name}`).then(result => {
                    vm.keysLoading = false;
                    if (result.code === -1) {
                        vm.$message.info(result.msg || 'Fail');
                        resolve([]);
                        return;
                    }
                    let data = (result.data || []).filter(item => !!item);
                    data.sort((a, b) => (a.key || '').localeCompare(b.key || ''));
                    resolve(data);
                }).catch(() => {
                    vm.keysLoading = false;
                    vm.$message.error('Error!');
                    resolve([]);
                });
            });
        },
        $get(name, key) {
            let vm = this;
            return new Promise((resolve) => {
                vm.redisLoading = true;
                vm._get(`get.json?name=${name}&key=${key}`).then(result => {
                    vm.redisLoading = false;
                    if (result.code === -1) {
                        vm.$message.info(result.msg || 'Fail');
                        resolve(null);
                        return;
                    }
                    resolve(result.data);
                }).catch(() => {
                    vm.redisLoading = false;
                    vm.$message.error('Error!');
                    resolve(null);
                });
            });
        },
        $fetch(name, key) {
            let vm = this;
            return new Promise((resolve) => {
                vm.hostsLoading = true;
                vm._get(`fetch.json?name=${name}&key=${key}`).then(result => {
                    vm.hostsLoading = false;
                    if (result.code === -1) {
                        vm.$message.info(result.msg || 'Fail');
                        resolve([]);
                        return;
                    }
                    let data = (result.data || []).filter(item => !!item);
                    data.sort((a, b) => (a.id || '').localeCompare(b.id || ''));
                    resolve(data);
                }).catch(() => {
                    vm.hostsLoading = false;
                    vm.$message.error('Error!');
                    resolve([]);
                });
            });
        },
        $rem(name) {
            const vm = this;
            return new Promise((resolve) => {
                vm._get(`rem.json?name=${name}`).then((result) => {
                    resolve(result.code !== -1);
                }).catch(() => {
                    resolve(false);
                });
            });
        },
        $del(name, key) {
            const vm = this;
            return new Promise((resolve) => {
                vm._get(`del.json?name=${name}&key=${key}`).then((result) => {
                    resolve(result.code !== -1);
                }).catch(() => {
                    resolve(false);
                });
            });
        },
        $cls() {
            const vm = this;
            return new Promise((resolve) => {
                vm._get(`cls.json`).then((result) => {
                    resolve(result.code !== -1);
                }).catch(() => {
                    resolve(false);
                });
            });
        },
        _get(url, opt) {
            const cfg = Object.assign({
                credentials: 'include'
            }, opt);
            const p1 = fetch(url, cfg).then(res => res.json());
            const p2 = new Promise(resolve => setTimeout(resolve, 600));
            return Promise.all([p1, p2]).then(result => Promise.resolve(result[0]));
        },
        _confirm(msg, title, cb) {
            const vm = this;
            vm.$confirm(msg, title, {
                confirmButtonText: 'YES',
                cancelButtonText: 'NO',
                type: 'warning'
            }).then(() => cb(true)).catch(() => cb(false));
        }
    }
});
