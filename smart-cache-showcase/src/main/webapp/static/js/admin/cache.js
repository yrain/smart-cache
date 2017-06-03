new Vue({
    el: '#app',
    data() {
        return {
            height: 0,
            names: [],
            keys: [],
            namesFilter: '',
            keysFilter: '',
            namesLoading: false,
            keysLoading: false,
            currentName: '',
            dialogFormVisible: false,
            data: '',
            title: ''
        }
    },
    mounted() {
        const vm = this;
        vm.handleResize();
        window.addEventListener('resize', vm.handleResize);
        vm.refreshNames();
    },
    computed: {
        filteredNames() {
            let vm = this;
            let filter = vm.namesFilter, keys = vm.names;
            return keys.filter(item => item.toLowerCase().indexOf(filter.toLowerCase()) !== -1);
        },
        filteredKeys() {
            let vm = this;
            let filter = vm.keysFilter, keys = vm.keys;
            return keys.filter(item => item.name.toLowerCase().indexOf(filter.toLowerCase()) !== -1);
        }
    },
    beforeDestroy() {
        window.removeEventListener('resize', this.handleResize);
    },
    methods: {
        handleSelect(name) {
            const vm = this;
            if (vm.currentName === name) {
                return;
            }
            vm.currentName = name;
            vm.refreshKeys();
        },
        handleResize() {
            const vm = this, $main = document.querySelector('#main'), $toolbar = document.querySelector('.toolbar');
            vm.height = $main.clientHeight - $toolbar.clientHeight;
        },
        handleGet(row) {
            const vm = this;
            vm.$get(vm.currentName, row.name).then(data => {
                vm.dialogFormVisible = true;
                vm.data = data;
                vm.title = 'Remote';
            });
        },
        handleFetch(row) {
            const vm = this;
            vm.$fetch(vm.currentName, row.name).then(data => {
                vm.dialogFormVisible = true;
                vm.data = data;
                vm.title = 'Local';
            });
        },
        handleDelete(row) {
            const vm = this;
            vm.$confirm('Remove It?', 'Confirm', {
                confirmButtonText: 'YES',
                cancelButtonText: 'NO',
                type: 'warning'
            }).then(() => {
                vm.$del(vm.currentName, row.name).then(success => {
                    if (!success) {
                        vm.$message.error('error.');
                        return;
                    }
                    vm.$message.info('success.');
                    vm.refreshKeys();
                });
            }).catch(() => {
            });
        },
        handleDeleteAll(name) {
            const vm = this;
            vm.$confirm('Remove It?', 'Confirm', {
                confirmButtonText: 'YES',
                cancelButtonText: 'NO',
                type: 'warning'
            }).then(() => {
                vm.$rem(name).then(success => {
                    if (!success) {
                        vm.$message.error('error.');
                        return;
                    }
                    vm.$message.info('success.');
                    vm.refreshNames();
                    vm.reset();
                });
            }).catch(() => {
            });
        },
        reset() {
            const vm = this;
            vm.keys = [];
            vm.keysFilter = '';
            vm.currentName = '';
        },
        refreshKeys() {
            const vm = this;
            if (!vm.currentName) {
                vm.$message.info('请在左侧菜单中选择一个!');
                return;
            }
            vm.$keys(vm.currentName).then(data => {
                vm.keys = data.map(item => {
                    return {
                        name: item,
                        data: {},
                        loading: false
                    };
                });
            });
        },
        refreshNames() {
            const vm = this;
            vm.$names().then(data => {
                vm.names = data;
            });
        },
        onOpen() {
            const vm = this;
            // 模拟 after open
            setTimeout(() => {
                const $content = document.getElementById('content');
                $content.innerHTML = '';
                const formatter = new JSONFormatter(vm.data);
                $content.appendChild(formatter.render());
                formatter.openAtDepth(2);
            });
        },
        $keys(name) {
            let vm = this;
            return new Promise((resolve) => {
                vm.keysLoading = true;
                fetch(`/showcase/admin/cache/keys?name=${name}`, {
                    credentials: 'include'
                }).then((res) => {
                    vm.keysLoading = false;
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    vm.keysLoading = false;
                    vm.$message.error('网络错误!');
                    resolve([]);
                });
            });
        },
        $names() {
            let vm = this;
            return new Promise((resolve) => {
                vm.namesLoading = true;
                fetch('/showcase/admin/cache/names', {
                    credentials: 'include'
                }).then((res) => {
                    vm.namesLoading = false;
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    vm.namesLoading = false;
                    vm.$message.error('网络错误!');
                    resolve([]);
                });
            });
        },
        $get(name, key) {
            let vm = this;
            return new Promise((resolve) => {
                fetch(`/showcase/admin/cache/get?name=${name}&key=${key}`, {
                    credentials: 'include'
                }).then((res) => {
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    vm.$message.error('网络错误!');
                    resolve([]);
                });
            });
        },
        $fetch(name, key) {
            let vm = this;
            return new Promise((resolve) => {
                fetch(`/showcase/admin/cache/fetch?name=${name}&key=${key}`, {
                    credentials: 'include'
                }).then((res) => {
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    vm.$message.error('网络错误!');
                    resolve([]);
                });
            });
        },
        $rem(name) {
            return new Promise((resolve) => {
                fetch(`/showcase/admin/cache/rem?name=${name}`, {
                    credentials: 'include'
                }).then(() => {
                    resolve(true);
                }).catch(() => {
                    resolve(false);
                });
            });
        },
        $del(name, key) {
            return new Promise((resolve) => {
                fetch(`/showcase/admin/cache/del?name=${name}&key=${key}`, {
                    credentials: 'include'
                }).then(() => {
                    resolve(true);
                }).catch(() => {
                    resolve(false);
                });
            });
        }
    }
});
