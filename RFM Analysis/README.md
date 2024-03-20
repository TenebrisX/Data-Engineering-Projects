# Проект 1-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 1-го спринта.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-1` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой локальный компьютер, в качестве пароля укажите ваш `Access Token` (получить нужно на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-1.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-1`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Как запустить контейнер
Запустите локально команду:

```
docker run -d --rm -p 15432:5432 -p 3000:3000 --name=de-project-sprint-1-server-local cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-1:latest
```

После того как запустится контейнер, у вас будут доступны:
1. VS Code
2. PostgreSQL (запросы лучше выполнять через DBeaver)

Параметры подключения:
* host: localhost
* port: 15432
* user: jovyan
* password: jovyan
* db: de
